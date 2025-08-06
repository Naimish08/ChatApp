import 'dotenv/config';
import express from 'express';
import axios from 'axios';
import multer from 'multer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import NodeCache from 'node-cache';
import { Queue, Worker } from 'bullmq';
import { GoogleGenerativeAIEmbeddings } from '@langchain/google-genai';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { QdrantVectorStore } from '@langchain/qdrant';

// Initialize Gemini client
const geminiClient = new GoogleGenerativeAI(process.env.GOOGLE_API_KEY);

const queue = new Queue('file-upload-queue', {
    connection: {
        host: process.env.REDIS_HOST || 'localhost',
        port: 6379,
        retryStrategy: (times) => {
            // Retry connection every 5 seconds
            return Math.min(times * 500, 2000);
        }
    }
});

// Setup __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Multer setup for destination directory: uploads/
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(__dirname, 'uploads');
    fs.mkdirSync(uploadDir, { recursive: true });
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    cb(null, file.originalname || `file_${Date.now()}`);
  },
});

const upload = multer({ storage });

// Initialize cache
const cache = new NodeCache({ stdTTL: 600 }); // 10 minute cache

// Initialize worker
const worker = new Worker('file-upload-queue', async (job) => {
    const { url, filename, savedTo } = JSON.parse(job.data);
    try {
        const embeddings = new GoogleGenerativeAIEmbeddings({
            apiKey: process.env.GOOGLE_API_KEY,
            model: 'models/embedding-001',
        });
        
        const vectorStore = await QdrantVectorStore.fromExistingCollection(
            embeddings,
            {
                url: `http://${process.env.QDRANT_HOST || 'localhost'}:6333`,
                collectionName: 'langchainjs-testing',
                timeout: 10000
            }
        );
        
        return { status: 'completed', vectorStore };
    } catch (error) {
        console.error('Worker error:', error);
        throw error;
    }
}, {
    connection: {
        host: process.env.REDIS_HOST || 'localhost',
        port: 6379
    }
});

const app = express();
app.use(express.json());

// Cache middleware
const cacheMiddleware = (req, res, next) => {
    if (req.method !== 'GET') return next();
    
    const key = req.originalUrl;
    const cachedResponse = cache.get(key);
    
    if (cachedResponse) {
        return res.json(cachedResponse);
    }
    next();
};

app.post('/api/v1/hackrx/run', async (req, res) => {
    const { documents, questions } = req.body;
    if (!documents || !questions) {
        return res.status(400).json({ error: 'Missing documents or questions' });
    }

    try {
        const cacheKey = `${documents}-${questions.join(',')}`;
        const cachedResult = cache.get(cacheKey);
        
        if (cachedResult) {
            return res.json(cachedResult);
        }

        const job = await queue.add('file-upload-queue', JSON.stringify({
            documents,
            questions,
            timestamp: new Date().toISOString()
        }), {
            attempts: 3,
            backoff: {
                type: 'exponential',
                delay: 1000
            }
        });

        // Return job ID and webhook URL
        const webhookUrl = `${req.protocol}://${req.get('host')}/api/v1/hackrx/status/${job.id}`;
        res.status(202).json({
            success: true,
            jobId: job.id,
            status: 'processing',
            webhookUrl
        });

    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ 
            success: false, 
            error: 'Failed to process request' 
        });
    }
});

// Add status endpoint
app.get('/api/v1/hackrx/status/:jobId', async (req, res) => {
    try {
        const job = await queue.getJob(req.params.jobId);
        if (!job) {
            return res.status(404).json({
                success: false,
                error: 'Job not found'
            });
        }

        const state = await job.getState();
        const result = await job.finished();

        res.json({
            success: true,
            jobId: job.id,
            state,
            result: result || null
        });

    } catch (error) {
        console.error('Status check error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get job status'
        });
    }
});

app.post('/hackrx/run', async (req, res) => {
  const { documents, questions } = req.body;
  if (!documents || !questions) {
    return res.status(400).json({ error: 'Missing documents or questions' });
  }

  try {
    // Download and process the document
    const response = await axios({
      method: 'GET',
      url: documents,
      responseType: 'stream',
    });

    const filename = `doc_${Date.now()}.pdf`;
    const uploadsDir = path.join(__dirname, 'uploads');
    const filePath = path.join(uploadsDir, filename);
    
    // Ensure uploads directory exists
    fs.mkdirSync(uploadsDir, { recursive: true });

    // Save the file
    const writer = fs.createWriteStream(filePath);
    response.data.pipe(writer);

    // Process file and handle questions
    await new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        try {
          // Queue the document processing
          const jobData = JSON.stringify({
            url: documents,
            filename,
            savedTo: filePath.replace(/\\/g, '/'),
          });
          
          await queue.add('file-upload-queue', jobData);

          // Wait for embeddings to be ready
          // TODO: Implement proper job completion check
          await new Promise(resolve => setTimeout(resolve, 5000));

          // Process all questions
          const embeddings = new GoogleGenerativeAIEmbeddings({
            apiKey: process.env.GOOGLE_API_KEY,
            model: 'models/embedding-001',
          });

          // Update Qdrant connection configuration
          const vectorStore = await QdrantVectorStore.fromExistingCollection(
            embeddings,
            {
                url: `http://${process.env.QDRANT_HOST || 'localhost'}:6333`,
                collectionName: 'langchainjs-testing',
                timeout: 10000 // 10 second timeout
            }
          );

          const answers = [];
          for (const question of questions) {
            const result = await vectorStore.similaritySearch(question, 2);
            
            const SYSTEM_PROMPT = `
            Answer the following question based on the provided context.
            Give a direct, factual answer without any additional text.
            don't add words like with the given context and all and also
            for yes/no qtn give detailed reason also
            
            Context:
            ${JSON.stringify(result)}
            
            Question: ${question}
            `;

            const model = geminiClient.getGenerativeModel({ model: "models/gemini-2.5-flash" });
            const chatResult = await model.generateContent([
              { text: SYSTEM_PROMPT }
            ]);
            const answer = chatResult.response.text();
            answers.push(answer);
          }

          res.json({
            answers: answers
          });
          resolve();
        } catch (error) {
          reject(error);
        }
      });

      writer.on('error', reject);
    });

  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Failed to process request' });
  }
});

// Add health check endpoint
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok' });
});

// Add error handling middleware
app.use((err, req, res, next) => {
    console.error('Error:', err);
    res.status(500).json({ 
        error: 'Internal Server Error',
        message: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
});

// Update server listening
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on http://0.0.0.0:${PORT}`);
});

// Add graceful shutdown
process.on('SIGTERM', async () => {
    console.log('Shutting down...');
    await worker.close();
    process.exit(0);
});
