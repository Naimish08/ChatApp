import 'dotenv/config';
import { Worker } from 'bullmq';
import { GoogleGenerativeAIEmbeddings } from '@langchain/google-genai';
import { QdrantVectorStore } from '@langchain/qdrant';
import { Document } from '@langchain/core/documents';
import { PDFLoader } from '@langchain/community/document_loaders/fs/pdf';
import { CharacterTextSplitter } from '@langchain/textsplitters';
import path from 'path';
import fs from 'fs';

const worker = new Worker(
  'file-upload-queue',
  async (job) => {
    console.log('🟢 Worker received new job');
    console.log('📄 Job data:', job.data);

    try {
      const data = JSON.parse(job.data);
      console.log('📋 Parsed job data:', data);

      // Check if file exists
      if (!fs.existsSync(data.savedTo)) {
        throw new Error(`PDF file not found at path: ${data.savedTo}`);
      }
      console.log('✅ File exists at path:', data.savedTo);

      // Load the PDF
      console.log('📚 Loading PDF...');
      const loader = new PDFLoader(data.savedTo);
      const docs = await loader.load();
      console.log('📄 PDF loaded successfully, pages:', docs.length);

      // Split the PDF into chunks
      console.log('✂️ Splitting document into chunks...');
      const splitter = new CharacterTextSplitter({
        chunkSize: 1000,
        chunkOverlap: 200,
      });
      const splitDocs = await splitter.splitDocuments(docs);
      console.log('🔪 Split complete. Number of chunks:', splitDocs.length);

      // Create embeddings and store in Qdrant
      console.log('🔄 Creating embeddings...');
      const embeddings = new GoogleGenerativeAIEmbeddings({
        apiKey: process.env.GOOGLE_API_KEY,
        model: 'models/embedding-001',
      });

      console.log('💾 Storing in Qdrant...');
      const vectorStore = await QdrantVectorStore.fromDocuments(
        splitDocs,
        embeddings,
        {
          url: 'http://localhost:6333',
          collectionName: 'langchainjs-testing',
        }
      );
      console.log('✅ Documents stored in Qdrant');

      // Verify storage
      console.log('🔍 Verifying storage with test search...');
      const testSearch = await vectorStore.similaritySearch('test', 1);
      console.log('🎯 Test search result:', JSON.stringify(testSearch, null, 2));
      
      return { success: true };
    } catch (err) {
      console.error('❌ Worker error:', err);
      console.error('Stack trace:', err.stack);
      throw err; // Rethrow to mark job as failed
    }
  },
  {
    connection: {
      host: 'localhost',
      port: '6379',
    },
  }
);

// Add error handler for the worker
worker.on('error', err => {
  console.error('🔴 Worker error event:', err);
});

// Add completed handler
worker.on('completed', job => {
  console.log('✅ Job completed:', job.id);
});

// Add failed handler
worker.on('failed', (job, err) => {
  console.error('❌ Job failed:', job.id, err);
});

console.log('🚀 Worker started and listening for jobs on queue: file-upload-queue');