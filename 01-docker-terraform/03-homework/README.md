# Data Engineering Zoomcamp - Homework 1 - Docker & Terraform


## Question 1. Understanding Docker images  
  
Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container.  
What's the version of pip in the image?  
* **25.3**
* 24.3.1  
* 24.2.1  
* 23.3.1  
  
```shell
docker run -it \
    --rm \
    --entrypoint=bash \
    python:3.13-slim

pip -V
```

## Question 2. Understanding Docker networking and docker-compose  

Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?  

* postgres:5433
* localhost:5432
* db:5433
* **postgres:5432**
* db:5432

**If multiple answers are correct, select any**  

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

Pgadmin connects to the database vm with name postgres. Port is the rightmost port in the mapping.  

## Data prep  
  
1. Start a fresh Git Codespace 
2. Create homework dir & copy files from docker workshop:  
  
```shell
.python-version
docker-compose.yaml
pyproject.toml
```
  
3. Start db & pgadmin 
  
```shell
docker-compose up -d
```
  
4. Install uv  
  
```shell
pip install uv
```

5. Get data  
  
```shell
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
```

```shell
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```
  
5. Run jupyter & login
  
```shell
uv run jupyter notebook
```
  
6. Explore data through jupyter & ingest to postgres. Key commands for notebook:  
  
```python
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
```
  
```python
df = pd.read_parquet('green_tripdata_2025-11.parquet', engine='pyarrow')
```
  
```python
df.head()
```
  
```python
df.shape
```
  
```python
df.dtypes
```
  
```python
engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')
```
  
```python
print(pd.io.sql.get_schema(df, name='green_trip_data', con=engine))
```
  
```python
# Create table with data
df.to_sql(
    name="green_trip_data",
    con=engine,
    if_exists="replace"
)
print("Table created")
```
  
```python
df = pd.read_csv('taxi_zone_lookup.csv')
```
  
```python
df.head()
```
  
```python
df.to_sql(
    name="taxi_zone_lookup",
    con=engine,
    if_exists="replace"
)
print("Table created") 
```

7. Connect to pgadmin to explore data & answer homework.  
  
- Connection link: [http://localhost:8085/browser/](http://localhost:8085/browser/) 
- Use credentials from docker-compose file / docker workshop.  

## Question 3. Counting short trips  
  
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?  
* 7,853  
* **8,007**
* 8,254  
* 8,421  
  
```sql
select count(*) from green_trip_data
where
	lpep_pickup_datetime >= '2025-11-01' and
	lpep_pickup_datetime < '2025-12-01' and
	trip_distance <= 1
```

## Question 4. Longest trip for each day  
  
Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).  
Use the pick up time for your calculations.  
* **2025-11-14**
* 2025-11-20  
* 2025-11-23  
* 2025-11-25  

```sql
select
	lpep_pickup_datetime,
	trip_distance
from
	green_trip_data
where
	trip_distance = (select max(trip_distance) from green_trip_data where trip_distance <= 100)
```

## Question 5. Biggest pickup zone  
  
Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?  
* ==East Harlem North==  
* East Harlem South  
* Morningside Heights  
* Forest Hills  

```sql
SELECT 
    tzl."Zone" AS zone,
    ROUND(CAST(SUM(gtd.total_amount) AS NUMERIC), 2) AS zone_amount
FROM
    green_trip_data AS gtd
    LEFT JOIN taxi_zone_lookup AS tzl
    ON gtd."PULocationID" = tzl."LocationID"
WHERE
    gtd.lpep_pickup_datetime::timestamp::date = '2025-11-18'
GROUP BY
    tzl."Zone"
ORDER BY
    zone_amount DESC
LIMIT 1;
```
  
Alternative:  
  
```sql
WITH zone_totals AS (
    SELECT 
        tzl."Zone" AS zone,
        ROUND(CAST(SUM(gtd.total_amount) AS NUMERIC), 2) AS zone_amount
    FROM green_trip_data AS gtd
    JOIN taxi_zone_lookup AS tzl ON gtd."PULocationID" = tzl."LocationID"
    WHERE gtd.lpep_pickup_datetime::timestamp::date = '2025-11-18'
    GROUP BY 1 -- Groups by the first column (tzl."Zone")
)
SELECT * FROM zone_totals 
ORDER BY zone_amount DESC 
LIMIT 1;
```
  
## Question 6. Largest tip  
  
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?  
Note: it's tip , not trip. We need the name of the zone, not the ID.  
* JFK Airport  
* **Yorkville West** 
* East Harlem North  
* LaGuardia Airport  
  
```sql
WITH zone_totals AS (
    SELECT 
        puz."Zone" AS pickup_zone,
		doz."Zone" AS dropoff_zone,
        MAX(gtd.tip_amount) as max_tip_amount
    FROM green_trip_data AS gtd
    LEFT JOIN taxi_zone_lookup AS puz ON gtd."PULocationID" = puz."LocationID"
	LEFT JOIN taxi_zone_lookup AS doz ON gtd."DOLocationID" = doz."LocationID"
    WHERE
		puz."Zone" = 'East Harlem North' AND
		gtd.lpep_pickup_datetime >= '2025-11-01' AND
		gtd.lpep_pickup_datetime < '2025-12-01'
    GROUP BY 1, 2
) 
SELECT * FROM zone_totals 
ORDER BY max_tip_amount DESC 
LIMIT 1;
```

## Question 7. Terraform Workflow  
  
Which of the following sequences, respectively, describes the workflow for:  
1. Downloading the provider plugins and setting up backend,  
2. Generating proposed changes and auto-executing the plan  
3. Remove all resources managed by terraform`

Answers:  
* terraform import, terraform apply -y, terraform destroy  
* teraform init, terraform plan -auto-apply, terraform rm  
* terraform init, terraform run -auto-approve, terraform destroy  
* **terraform init, terraform apply -auto-approve, terraform destroy**
* terraform import, terraform apply -y, terraform rm

Selected this answer because all other include commands that do not exist within terraform: import, rm, run.
