import os
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from sklearn.cluster import DBSCAN
import numpy as np
import certifi

# --- Configuration ---
load_dotenv()

# MongoDB Configuration - pulls from the same .env file
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_app")
ARTICLES_COLLECTION_NAME = "articles"
SUMMARIES_COLLECTION_NAME = "summaries" # Collection where main.py saves summaries

# Clustering Configuration
DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 2 # Minimum number of articles to form an event cluster

def run_test():
    """
    Connects to MongoDB, runs clustering, finds the existing summary for each
    cluster, and saves the results to local JSON files.
    """
    print("--- Running Cluster Test Script ---")

    # --- Connect to MongoDB ---
    try:
        mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
        db = mongo_client[MONGO_DB_NAME]
        articles_collection = db[ARTICLES_COLLECTION_NAME]
        summaries_collection = db[SUMMARIES_COLLECTION_NAME]
        # Verify connection
        mongo_client.admin.command('ismaster')
        print("Successfully connected to MongoDB.")
    except Exception as e:
        print(f"Could not connect to MongoDB. Error: {e}")
        return

    # 1. Create clusters directory if it doesn't exist
    output_dir = "clusters"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created '{output_dir}' directory.")

    # 2. Fetch all articles that have an embedding to test clustering
    articles_to_cluster = list(articles_collection.find({"embedding": {"$exists": True}}))
    
    if len(articles_to_cluster) < DBSCAN_MIN_SAMPLES:
        print("Not enough articles with embeddings to form a cluster.")
        return

    print(f"Found {len(articles_to_cluster)} articles with embeddings to test.")
    
    # 3. Perform clustering
    embeddings = np.array([article['embedding'] for article in articles_to_cluster])
    clustering = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES, metric='cosine').fit(embeddings)
    labels = clustering.labels_
    
    clusters = {}
    for i, article in enumerate(articles_to_cluster):
        label = labels[i]
        if label != -1: # Ignore noise points
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(article)
    
    if not clusters:
        print("No clusters were formed.")
        return

    print(f"Formed {len(clusters)} clusters. Saving them to JSON files...")
    
    # 4. For each cluster, find its summary in the DB and save to a file
    for label, articles_in_cluster in clusters.items():
        cluster_file_path = os.path.join(output_dir, f"cluster_{label}.json")
        
        # --- Fetch the existing summary ---
        summary_data = {"headline": "No summary found in DB", "summary": "This cluster may not have been processed by the main synthesizer yet."}
        
        # Get the cluster_id from the first article (they should all be the same)
        first_article = articles_in_cluster[0]
        if "cluster_id" in first_article:
            cluster_id = first_article["cluster_id"]
            found_summary = summaries_collection.find_one({"_id": cluster_id})
            if found_summary:
                summary_data = {
                    "headline": found_summary.get("headline"),
                    "summary": found_summary.get("summary")
                }

        # --- Prepare article data for saving ---
        article_data_to_save = []
        for article in articles_in_cluster:
            article_data_to_save.append({
                "headline": article.get("headline"),
                "url": article.get("url"),
                "author": article.get("author"),
                "body_snippet": article.get("body", "")[:200]
            })
        
        # --- Combine summary and articles into one object ---
        final_output = {
            "retrieved_summary": summary_data,
            "articles": article_data_to_save
        }
            
        with open(cluster_file_path, "w") as f:
            json.dump(final_output, f, indent=4, default=str) # Use default=str to handle ObjectId
        
        print(f"Saved cluster {label}.")
            
    print(f"Successfully saved {len(clusters)} clusters to the '{output_dir}' directory.")


if __name__ == "__main__":
    run_test()

