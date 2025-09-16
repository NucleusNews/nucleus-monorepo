import express from 'express';
import cors from 'cors';
import { connectToServer, getDb } from './db.js'; // Import your DB functions

const app = express();
const port = 8080;
const host = '0.0.0.0';

app.use(cors());
app.use(express.json());

// Example route to test the database connection
app.get('/api/stories', async (req, res) => {
  try {
    // Get the database instance and find the 'stories' collection
    const db = getDb();
    const stories = await db.collection("stories").find({}).toArray();
    res.json(stories);
  } catch (err) {
    console.error("Failed to fetch stories:", err);
    res.status(500).json({ message: "Failed to fetch stories" });
  }
});

// Start the server AFTER connecting to the database
connectToServer().then(() => {
  app.listen(port, host, () => {
    console.log(`Server is running on http://localhost:${port}`);
  });
});