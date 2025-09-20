import os
import time
import json
from dotenv import load_dotenv
from upstash_redis import Redis
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
import certifi # Import the certifi library

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# Redis Configuration
REDIS_QUEUE_KEY = "raw_articles_queue"

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_app")
MONGO_COLLECTION_NAME = "articles" # Where processed articles will be stored

# --- Connections ---
# Connect to Upstash Redis
try:
    redis_client = Redis.from_env()
    redis_client.ping()
    print("Successfully connected to Upstash Redis.")
except Exception as e:
    print(f"Could not connect to Upstash Redis. Check your .env file. Error: {e}")
    exit(1)

# Connect to MongoDB
try:
    # Use certifi to provide the SSL certificate authority
    mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = mongo_client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    # The ismaster command is cheap and does not require auth.
    mongo_client.admin.command('ismaster')
    print("Successfully connected to MongoDB.")
except Exception as e:
    print(f"Could not connect to MongoDB. Check your MONGO_URI. Error: {e}")
    exit(1)

# --- AI Model Loading ---
# Load a pre-trained sentence-transformer model.
# This model will convert text into a 384-dimensional vector.
try:
    print("Loading sentence-transformer model...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    print("Model loaded successfully.")
except Exception as e:
    print(f"Failed to load sentence-transformer model. Make sure you have an internet connection. Error: {e}")
    exit(1)


def process_article(article_data):
    """
    Generates a vector embedding for the article and stores it in MongoDB.
    """
    try:
        # Combine headline and the first few sentences of the body for a representative embedding
        text_to_embed = f"{article_data.get('headline', '')}. {article_data.get('body', '')[:500]}"
        
        # Generate the embedding
        embedding = model.encode(text_to_embed, convert_to_tensor=False).tolist()
        
        # Prepare the document for MongoDB
        processed_document = {
            **article_data, # Include all original data
            "embedding": embedding,
            "processed_at": time.time()
        }
        
        # Insert into MongoDB
        collection.insert_one(processed_document)
        print(f"Processed and stored article: {article_data.get('headline')}")
        
    except Exception as e:
        print(f"An error occurred while processing article {article_data.get('url')}: {e}")

if __name__ == "__main__":
    print("Starting the processor service...")
    while True:
        try:
            # RPOP pops an item from the right of the list (FIFO).
            # It's a blocking pop (BRPOP) if you want it to wait, but polling is fine too.
            raw_article_json = redis_client.rpop(REDIS_QUEUE_KEY)
            
            if raw_article_json:
                article_data = json.loads(raw_article_json)
                process_article(article_data)
            else:
                # If the queue is empty, wait a bit before checking again.
                print("Queue is empty. Waiting for new articles...")
                time.sleep(10) # Wait for 10 seconds

        except Exception as e:
            print(f"An unexpected error occurred in the main loop: {e}")
            time.sleep(30) # Wait a bit longer on an unexpected error

