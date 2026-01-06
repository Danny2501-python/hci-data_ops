#!/usr/bin/env python3
"""
API to MinIO ETL
Fetch data from API and save to MinIO raw-zone bucket
"""
import os
import sys
import json
from datetime import datetime
import requests
from minio import Minio

# Configuration from environment variables
API_URL = os.getenv('API_URL', 'https://jsonplaceholder.typicode.com/posts')
S3_ENDPOINT = os.getenv('S3_ENDPOINT', '10.152.183.65:9000')
S3_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
S3_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
S3_BUCKET = 'raw-zone'

def log(message):
    """Simple logging"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def fetch_api_data(url):
    """Fetch data from API"""
    log(f"Fetching data from: {url}")
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    record_count = len(data) if isinstance(data, list) else 1
    log(f"Fetched {record_count} records")
    
    return data

def save_to_minio(data, object_name):
    """Save data to MinIO"""
    log(f"Saving to MinIO: {S3_BUCKET}/{object_name}")
    
    # Initialize MinIO client
    client = Minio(
        S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=False
    )
    
    # Ensure bucket exists
    if not client.bucket_exists(S3_BUCKET):
        client.make_bucket(S3_BUCKET)
        log(f"Created bucket: {S3_BUCKET}")
    
    # Save as JSON
    json_file = '/tmp/data.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    # Upload to MinIO
    client.fput_object(S3_BUCKET, object_name, json_file)
    log(f"✅ Uploaded to MinIO: {S3_BUCKET}/{object_name}")
    
    # Cleanup
    os.remove(json_file)

def main():
    """Main execution"""
    log("=" * 60)
    log("Starting API to MinIO ETL")
    log("=" * 60)
    
    try:
        # Get execution date from command line or use today
        execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
        log(f"Execution date: {execution_date}")
        
        # Fetch data from API
        data = fetch_api_data(API_URL)
        
        # Save to MinIO
        object_name = f"api_data/{execution_date}/data.json"
        save_to_minio(data, object_name)
        
        log("=" * 60)
        log("✅ ETL completed successfully!")
        log("=" * 60)
        
    except Exception as e:
        log(f"❌ ETL failed: {str(e)}")
        raise

if __name__ == '__main__':
    main()
