import os
import time
import requests
import json
from dotenv import load_dotenv
from upstash_redis import Redis

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# API Keys
GUARDIAN_API_KEY = os.getenv("GUARDIAN_API_KEY")
THENEWSAPI_API_KEY = os.getenv("THENEWSAPI_API_KEY")

# API Endpoints
GUARDIAN_API_ENDPOINT = "https://content.guardianapis.com/search"
THENEWSAPI_ENDPOINT = "https://api.thenewsapi.com/v1/news/all"

# Redis Configuration
REDIS_QUEUE_KEY = "raw_articles_queue"
REDIS_SEEN_URLS_KEY = "seen_articles_urls"

# Check for API Keys
if not GUARDIAN_API_KEY:
    print("Warning: GUARDIAN_API_KEY is not set. The Guardian fetcher will be skipped.")
if not THENEWSAPI_API_KEY:
    print("Warning: THENEWSAPI_API_KEY is not set. TheNewsAPI fetcher will be skipped.")

# Connect to Redis
try:
    redis_client = Redis.from_env()
    redis_client.ping()
    print("Successfully connected to Upstash Redis.")
except Exception as e:
    print(f"Could not connect to Upstash Redis. Check your .env file and credentials. Error: {e}")
    exit(1)


def fetch_guardian_articles():
    """
    Fetches the latest articles from The Guardian API.
    """
    if not GUARDIAN_API_KEY:
        return

    print("--- Fetching latest articles from The Guardian ---")
    page = 1
    total_new_articles = 0
    MAX_PAGES_TO_FETCH = 10

    while page <= MAX_PAGES_TO_FETCH:
        params = {
            'api-key': GUARDIAN_API_KEY,
            'order-by': 'newest',
            'show-fields': 'headline,bodyText,byline',
            'page-size': 50,
            'page': page
        }
        try:
            print(f"Fetching page {page}...")
            response = requests.get(GUARDIAN_API_ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json().get('response', {})
            articles = data.get('results', [])
            
            if not articles:
                print("No more articles found on this page.")
                break

            found_new_on_this_page = False
            for article in articles:
                article_url = article.get('webUrl')
                if article_url and not redis_client.sismember(REDIS_SEEN_URLS_KEY, article_url):
                    found_new_on_this_page = True
                    total_new_articles += 1
                    
                    article_payload = {
                        'source': 'The Guardian',
                        'url': article_url,
                        'headline': article.get('fields', {}).get('headline', ''),
                        'body': article.get('fields', {}).get('bodyText', ''),
                        'author': article.get('fields', {}).get('byline', 'N/A'),
                        'published_at': article.get('webPublicationDate')
                    }
                    
                    redis_client.lpush(REDIS_QUEUE_KEY, json.dumps(article_payload))
                    redis_client.sadd(REDIS_SEEN_URLS_KEY, article_url)

            if not found_new_on_this_page:
                print("No new articles found on this page. Stopping pagination.")
                break
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data from The Guardian: {e}")
            break
    print(f"The Guardian: Found and queued {total_new_articles} new articles.")


def fetch_thenewsapi_articles():
    """
    Fetches the latest articles from The News API by iterating through pages.
    """
    if not THENEWSAPI_API_KEY:
        return
        
    print("--- Fetching latest articles from The News API ---")
    total_new_articles = 0
    page = 1
    MAX_PAGES_TO_FETCH = 10 # Add a hard limit to avoid excessive requests

    while page <= MAX_PAGES_TO_FETCH:
        params = {
            'api_token': THENEWSAPI_API_KEY,
            'language': 'en',
            'page': page,
            'limit': 50 # Request up to 50, but API may return fewer on free plan
        }
        try:
            print(f"Fetching page {page}...")
            response = requests.get(THENEWSAPI_ENDPOINT, params=params)
            response.raise_for_status()
            articles = response.json().get('data', [])

            if not articles:
                print("No more articles found on this page.")
                break

            found_new_on_this_page = False
            for article in articles:
                article_url = article.get('url')
                if article_url and not redis_client.sismember(REDIS_SEEN_URLS_KEY, article_url):
                    found_new_on_this_page = True
                    total_new_articles += 1
                    
                    author_name = article.get('author') or article.get('source') or 'N/A'

                    article_payload = {
                        'source': article.get('source', 'The News API'),
                        'url': article_url,
                        'headline': article.get('title', ''),
                        'body': article.get('description', '') + '\n' + article.get('snippet', ''),
                        'author': author_name,
                        'published_at': article.get('published_at')
                    }

                    redis_client.lpush(REDIS_QUEUE_KEY, json.dumps(article_payload))
                    redis_client.sadd(REDIS_SEEN_URLS_KEY, article_url)
            
            if not found_new_on_this_page:
                print("No new articles found on this page. Stopping pagination.")
                break
            
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data from The News API: {e}")
            break
    
    print(f"The News API: Found and queued {total_new_articles} new articles.")


if __name__ == "__main__":
    POLLING_INTERVAL_SECONDS = 900
    
    print("Starting the ingestor service...")
    while True:
        fetch_guardian_articles()
        fetch_thenewsapi_articles()
        print(f"Waiting for {POLLING_INTERVAL_SECONDS // 60} minutes before next fetch...")
        time.sleep(POLLING_INTERVAL_SECONDS)

