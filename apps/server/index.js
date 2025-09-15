import express from 'express';
import cors from 'cors';

const app = express();
const port = 8080;

app.use(cors());
app.use(express.json());

app.get('/api', (req, res) => {
  res.json({ message: 'Hello from the server! 👋' });
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
