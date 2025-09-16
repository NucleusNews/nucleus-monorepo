import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

const uri = process.env.MONGO_URI;
if (!uri) {
  throw new Error('MONGO_URI is not defined in the .env file');
}

const client = new MongoClient(uri);

let db;

// Function to connect to the database
export const connectToServer = async () => {
  try {
    // Connect the client to the server
    await client.connect();
    // Establish and verify connection
    db = client.db("news_app"); // Specify your database name here
    console.log("Successfully connected to MongoDB.");
  } catch (err) {
    console.error(err);
  }
};

// Function to get the database instance
export const getDb = () => {
  return db;
};