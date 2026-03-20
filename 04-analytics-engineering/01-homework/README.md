# Data Engineering Zoomcamp - Homework 4: Analytics Engineering & dbt


In this homework, we'll use the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. Set up your dbt project following the [setup guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/setup)
2. Load the Green and Yellow taxi data for 2019-2020 and FHV trip data for 2019 into your warehouse (use static tables from [dtc github](https://github.com/DataTalksClub/nyc-tlc-data/), don't use offical tables from tlc because some values change from time to time)
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

### Steps I took for setup

1. Create project 'de-zoomcamp' on Google Cloud Platform
2. Create 2 service accounts:
	- data-load-service:
		- permissions: Storage Admin
		- used for loading data to GCS data lake
		- create and download JSON key
	- dbt-service:
		- permissions: BigQuery Data Editor, BigQuery Job User, BigQuery User
		- used for dbt
		- create and download JSON key
3. Load data to GCP data lake with a Python script below:

```python
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
```

4. Verify the data is in GCP data lake through Google Cloud Console (GCC).
5. Create dataset 'nytaxi' through GCC, set location to 'EU'.
6. Run SQL code to create external & materialised tables within BigQuery:

```sql
-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-489904.nytaxi.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tlc-data-lake/green/green_tripdata_20*.csv.gz']
);

-- Create Native Table (Materialized)
CREATE OR REPLACE TABLE `de-zoomcamp-489904.nytaxi.green_tripdata` AS
SELECT * FROM `de-zoomcamp-489904.nytaxi.external_green_tripdata`;
```

```sql
-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-489904.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tlc-data-lake/yellow/yellow_tripdata_20*.csv.gz']
);

-- Create Native Table (Materialized)
CREATE OR REPLACE TABLE `de-zoomcamp-489904.nytaxi.yellow_tripdata` AS
SELECT * FROM `de-zoomcamp-489904.nytaxi.external_yellow_tripdata`;
```

```sql
-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-489904.nytaxi.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tlc-data-lake/fhv/fhv_tripdata_20*.csv.gz']
);

-- Create Native Table (Materialized)
CREATE OR REPLACE TABLE `de-zoomcamp-489904.nytaxi.fhv_tripdata` AS
SELECT * FROM `de-zoomcamp-489904.nytaxi.external_fhv_tripdata`;
```

7. Verify the data tables have been set up through GCC.
8. Setup dbt project taxi_rides_ny using the course instructions steps [3](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/setup/cloud_setup.md#step-3-create-a-new-dbt-project), [4](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/setup/cloud_setup.md#step-4-configure-bigquery-connection) and [5](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/setup/cloud_setup.md#step-5-set-up-your-repository)
9. Updated dbt project settings to use '04-analytics-engineering/01-homework' as a project subdirectory. Now dbt is configured to talk to my BigQuery dataset and my GitHub repo.
10. Install dbt cloud cli
11. Clone project files from Github and update for cloud CLI, project name, and download dbt_cloud.yml.
12. Run dbt build


---

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- **`int_trips_unioned` only**
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

---

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- **dbt will fail the test, returning a non-zero exit code**
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- **12,184**
- 15,421

I looked up the total count of rows in the `Details` page of `fct_monthly_zone_revenue` table in GCP.

---

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- **East Harlem North**
- Morningside Heights
- East Harlem South
- Washington Heights South

I selected the top row from this query:

```sql
Select
  pickup_zone,
  sum(revenue_monthly_total_amount) as total_revenue
from dbt_vb.fct_monthly_zone_revenue
where
  service_type = 'Green' and
  extract(year from revenue_month) = 2020
group by
  pickup_zone
order by
  total_revenue desc
```


---

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 500,234
- 350,891
- **384,624**
- 421,509

I used the query below:

```sql
Select
	sum(total_monthly_trips) as total_trips
from dbt_vb.fct_monthly_zone_revenue
where
	service_type = 'Green' and
	revenue_month = '2019-10-01'
order by
	total_trips desc
```


---

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
    - Filter out records where `dispatching_base_num IS NULL`
    - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- **43,244,693**
- 22,998,722
- 44,112,187

```sql
SELECT count(*) FROM `de-zoomcamp-489904.dbt_vb.stg_fhv_tripdata`
```
