import 'dotenv/config';
import express from 'express';
import axios from 'axios';
import multer from 'multer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Queue } from 'bullmq';
import { GoogleGenerativeAIEmbeddings } from '@langchain/google-genai';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { QdrantVectorStore } from '@langchain/qdrant';

// Initialize Gemini client
const geminiClient = new GoogleGenerativeAI(process.env.GOOGLE_API_KEY);

const queue = new Queue('file-upload-queue',{
    connection: {
        host: 'localhost',
        port: 6379, 
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

const app = express();
app.use(express.json());

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

          const vectorStore = await QdrantVectorStore.fromExistingCollection(
            embeddings,
            {
              url: 'http://localhost:6333',
              collectionName: 'langchainjs-testing',
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

app.listen(3000, () => {
  console.log('Server running on http://localhost:3000');
});
