import os
import time
import json
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from sklearn.cluster import DBSCAN
import numpy as np
import google.generativeai as genai
import certifi

# --- Configuration ---
load_dotenv()

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_app")
ARTICLES_COLLECTION_NAME = "articles"
SUMMARIES_COLLECTION_NAME = "summaries" # Where final stories are saved

# Gemini API Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY is not set in the .env file.")
genai.configure(api_key=GEMINI_API_KEY)

# Clustering Configuration
DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 2 # Minimum number of articles to form an event cluster

# --- Connections ---
try:
    mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = mongo_client[MONGO_DB_NAME]
    articles_collection = db[ARTICLES_COLLECTION_NAME]
    summaries_collection = db[SUMMARIES_COLLECTION_NAME]
    mongo_client.admin.command('ismaster')
    print("Successfully connected to MongoDB.")
except Exception as e:
    print(f"Could not connect to MongoDB. Error: {e}")
    exit(1)

# --- LLM Model ---
try:
    llm = genai.GenerativeModel('gemini-1.5-flash')
    print("Gemini model loaded successfully.")
except Exception as e:
    print(f"Could not load Gemini model. Error: {e}")
    exit(1)


def summarize_articles(articles):
    """
    Uses the Gemini API to generate a summary for a cluster of articles.
    """
    print(f"Summarizing {len(articles)} articles...")
    
    combined_text = ""
    for article in articles:
        combined_text += f"Headline: {article.get('headline', '')}\n"
        combined_text += f"Body: {article.get('body', '')[:1000]}\n---\n"

    prompt = f"""
    You are a neutral news editor. The following text contains multiple news articles covering the same event. 
    Your task is to synthesize all the information into a single, comprehensive, and factual summary. 
    Do not include speculation. Respond ONLY with a JSON object with keys: "headline", "summary", and "tags".
    The headline should be a short, descriptive title for the event.
    The summary should be 3-4 sentences long.
    The tags should be an array of 3-5 relevant keywords.

    Here are the articles:
    ---
    {combined_text}
    ---
    """

    try:
        response = llm.generate_content(prompt)
        summary_json = json.loads(response.text.strip('```json\n').strip('```'))
        return summary_json
    except Exception as e:
        print(f"An error occurred while calling the Gemini API: {e}")
        return None

def run_clustering_and_summarization():
    """
    Fetches unclustered articles, performs DBSCAN clustering, and summarizes each cluster.
    """
    print("Starting clustering and summarization job...")
    
    unclustered_articles = list(articles_collection.find({"cluster_id": {"$exists": False}}))
    
    if len(unclustered_articles) < DBSCAN_MIN_SAMPLES:
        print(f"Not enough new articles to form a cluster. Found {len(unclustered_articles)}, need at least {DBSCAN_MIN_SAMPLES}.")
        return

    print(f"Found {len(unclustered_articles)} unclustered articles.")
    
    embeddings = np.array([article['embedding'] for article in unclustered_articles])
    
    clustering = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES, metric='cosine').fit(embeddings)
    labels = clustering.labels_
    
    clusters = {}
    for i, article in enumerate(unclustered_articles):
        label = labels[i]
        if label != -1:
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(article)
    
    if not clusters:
        print("No new event clusters were formed.")
        return

    print(f"Formed {len(clusters)} new event clusters.")
    
    bulk_updates = []
    for label, articles_in_cluster in clusters.items():
        summary_data = summarize_articles(articles_in_cluster)
        
        if summary_data:
            article_ids = [article['_id'] for article in articles_in_cluster]
            
            summary_doc = {
                "headline": summary_data.get('headline'),
                "summary": summary_data.get('summary'),
                "tags": summary_data.get('tags', []),
                "created_at": time.time(),
                "related_article_ids": article_ids,
                "article_count": len(article_ids)
            }
            summary_result = summaries_collection.insert_one(summary_doc)
            print(f"Saved summary for cluster {label} with ID: {summary_result.inserted_id}")
            
            for article_id in article_ids:
                bulk_updates.append(UpdateOne({"_id": article_id}, {"$set": {"cluster_id": summary_result.inserted_id}}))

        # Respect the API rate limit before the next iteration
        # The free tier allows about 15 requests/minute, so waiting 5 seconds is safe.
        print("Waiting for 5 seconds to respect API rate limit...")
        time.sleep(5)

    if bulk_updates:
        articles_collection.bulk_write(bulk_updates)
        print(f"Successfully marked {len(bulk_updates)} articles as clustered.")


if __name__ == "__main__":
    JOB_INTERVAL_SECONDS = 1800 # Run every 30 minutes
    
    print("Starting the synthesizer service...")
    while True:
        run_clustering_and_summarization()
        print(f"Job complete. Waiting for {JOB_INTERVAL_SECONDS // 60} minutes...")
        time.sleep(JOB_INTERVAL_SECONDS)

