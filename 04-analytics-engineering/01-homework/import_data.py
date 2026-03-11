import os
import json
import requests
from google.cloud import storage
from google.oauth2 import service_account

# Configuration
PROJECT_ID = "de-zoomcamp-489904"
BUCKET_NAME = "nyc-tlc-data-lake" # Change this to your preferred bucket name
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# Dataset Map: { 'color/type': [years] }
DATASETS = {
    "yellow": [2019, 2020],
    "green": [2019, 2020],
    "fhv": [2019]
}

def get_gcs_client():
    """Initializes GCS client using the DLS_CREDS env var."""
    credentials_dict = json.loads(os.environ["DLS_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    return storage.Client(credentials=credentials, project=PROJECT_ID)

def upload_to_gcs(bucket, object_name, url):
    """Downloads file in chunks and streams it directly to GCS."""
    print(f"Processing {object_name}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        blob = bucket.blob(object_name)
        blob.upload_from_string(response.content, content_type='text/csv')
        print(f"✅ Uploaded to gs://{BUCKET_NAME}/{object_name}")
    else:
        print(f"❌ Failed to download {url} (Status: {response.status_code})")

def main():
    client = get_gcs_client()
    
    # Create bucket if it doesn't exist
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        bucket = client.create_bucket(BUCKET_NAME, location="EU")
        print(f"Created bucket {BUCKET_NAME}")

    for service, years in DATASETS.items():
        for year in years:
            for month in range(1, 13):
                # Formatting names (e.g., yellow_tripdata_2019-01.csv.gz)
                file_name = f"{service}_tripdata_{year}-{month:02d}.csv.gz"
                url = f"{BASE_URL}/{service}/{file_name}"
                
                # Check for FHV specific URL structure if needed, 
                # but the GitHub releases usually follow this pattern.
                upload_to_gcs(bucket, f"{service}/{file_name}", url)

if __name__ == "__main__":
    main()