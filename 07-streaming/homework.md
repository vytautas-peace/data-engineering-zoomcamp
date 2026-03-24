# Data Engineering Zoomcamp - Homework 6 - Redpanda / Kafka and Pyflink

# Homework

In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

- [green_tripdata_2025-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet)


## Setup

We'll use the same infrastructure from the [workshop](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/07-streaming/workshop).

Follow the setup instructions: build the Docker image, start the services:

```shell
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

This gives us:

- Redpanda (Kafka-compatible broker) on `localhost:9092`
- Flink Job Manager at [http://localhost:8081](http://localhost:8081/)
- Flink Task Manager
- PostgreSQL on `localhost:5432` (user: `postgres`, password: `postgres`)

If you previously ran the workshop and have old containers/volumes, do a clean start:

```shell
docker compose down -v
docker compose build
docker compose up -d
```

Note: the container names (like `workshop-redpanda-1`) assume the directory is called `workshop`. If you renamed it, adjust accordingly.


## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```shell
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

### Answer

I ran command:

```shell
docker exec -it 07-streaming-redpanda-1 rpk version
```

That shows Redpanda version v25.3.9.

## Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

```shell
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic. You'll need to handle the datetime columns - convert them to strings before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

- 10 seconds
- **60 seconds**
- 120 seconds
- 300 seconds

### Answer

52 seconds.

To get the answer...

I ran command:

```shell
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips
```

Then updated the [producer_hw.py](https://github.com/vytautas-peace/data-engineering-zoomcamp/blob/main/07-streaming/src/producers/producer_hw.py)  and [models_hw.py](https://github.com/vytautas-peace/data-engineering-zoomcamp/blob/main/07-streaming/src/models/models_hw.py) scripts with new fields and url.

Then ran the producer script:

```shell
uv run python src/producers/producer_hw.py
```


## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic (set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- **8506**
- 9506

### Answer

8506.

I updated / created [consumer_hw.py](https://github.com/vytautas-peace/data-engineering-zoomcamp/blob/main/07-streaming/src/consumers/consumer_hw.py) that references models_hw.py

Then created table in PG:

```sql
CREATE TABLE processed_events (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
	tip_amount DOUBLE PRECISION,
	total_amount DOUBLE PRECISION
);
```

Then ran consumer_hw and producer_hw scripts.

Then ran SQL through PGCLI:

```sql
SELECT COUNT(*)
FROM processed_events
WHERE trip_distance > 3.10686;
```

Figure 3.10686 is 5 km in miles.

The answer I got is 15,491, which is not one of the options.

Changing the condition to 'trip_distance > 5' produces 8506, which is one of the answers, so choosing this one.


## Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, you'll adapt the workshop code to work with the green taxi data. The key differences from the workshop:

- Topic name: `green-trips` (instead of `rides`)
- Datetime columns use `lpep_` prefix (instead of `tpep_`)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables for your results.

Important notes for the Flink jobs:

- Place your job files in `workshop/src/job/` - this directory is mounted into the Flink containers at `/opt/src/job/`
- Submit jobs with: `docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/your_job.py`
- The `green-trips` topic has 1 partition, so set parallelism to 1 in your Flink jobs (`env.set_parallelism(1)`). With higher parallelism, idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute or two until results appear in PostgreSQL, then query the results. You can cancel the job from the Flink UI at [http://localhost:8081](http://localhost:8081/)
- If you sent data to the topic multiple times, delete and recreate the topic to avoid duplicates: `docker exec -it workshop-redpanda-1 rpk topic delete green-trips`

### Setup

1. Start the 4 images.

```shell
docker compose up -d
```

2. Verify all images are working.

```shell
docker compose ps
```

3. Forward port 8081 for Flink.

4. Create `green-trips` topic.

```shell
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips
```

5. Run pgcli.

```shell
uvx pgcli -h localhost -p 5432 -U postgres -d postgres
# password: postgres
```


## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns: `window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- **74**
- 75
- 166

### Answer

1. Pgcli - create results table.

```sql
CREATE TABLE output_q4 (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);
```


2. Terminal - submit [hw_q4_job.py](https://github.com/vytautas-peace/data-engineering-zoomcamp/blob/main/07-streaming/src/jobs/hw_q4_job.py) job to Flink.

```shell
docker exec -it 07-streaming-jobmanager-1 flink run \
	-py /opt/src/jobs/hw_q4_job.py\
	--pyFiles /opt/src -d
```

3. Terminal - run the producer_hw.py script.

```shell
uv run python src/producers/producer_hw.py
```

4. Pgcli - run SQL query.

```sql
SELECT PULocationID, num_trips
FROM output_q4
ORDER BY num_trips DESC
LIMIT 3;
```


## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap on `PULocationID`, using `lpep_pickup_datetime` as the event time with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other. When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID` with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- **51**
- 81

### Answer

1. Updated job script switching from tumble window to session: [hw_q5_job.py](https://github.com/vytautas-peace/data-engineering-zoomcamp/blob/main/07-streaming/src/jobs/hw_q5_job.py).
2. Cancel any Flink jobs.
3. T1:docker - delete + recreate green-trips topic.

```shell
docker exec -it 07-streaming-redpanda-1 rpk topic delete green-trips && \
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips
```

3. T2:pgcli - drop output table and create it with window_end to measure session length.

```sql
CREATE TABLE output_q5 (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);
```

4. Submit Flink job.

```shell
docker exec -it 07-streaming-jobmanager-1 flink run \
	-py /opt/src/jobs/hw_q5_job.py \
	--pyFiles /opt/src \
	-d \
```

5. Run the producer.

```shell
uv run python src/producers/producer_hw.py
```

6. Measure results.

```sql
SELECT
	PULocationID,
	sum(num_trips) as num_trips,
	window_start,
	(window_end - window_start) as session_duration
FROM output_q5
GROUP BY
	PULocationID, window_start
ORDER BY
	(window_end - window_start) DESC
LIMIT 10;
```

7. Some verification queries.

```sql
SELECT sum(num_trips) FROM output_q5;
SELECT COUNT(*) FROM output_q5;
```

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- **2025-10-16 18:00:00**
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00


### Answer

1. Updated job script switching from tumble window to session: [hw_q6_job.py](https://github.com/vytautas-peace/data-engineering-zoomcamp/blob/main/07-streaming/src/jobs/hw_q6_job.py).
2. Reset docker & create topic & create output table & submit job. Combined the commands cause it's a bit much otherwise.

```shell
docker compose down -v && \
docker compose up -d && \
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips && \
docker exec -it 07-streaming-postgres-1 psql -U postgres -c "CREATE TABLE output_q6 (window_start TIMESTAMP, total_tips DOUBLE PRECISION, PRIMARY KEY (window_start));" && \
docker exec -it 07-streaming-jobmanager-1 flink run \
	-py /opt/src/jobs/hw_q6_job.py \
	--pyFiles /opt/src \
	-d
```

3. Run the producer.

```shell
uv run python src/producers/producer_hw.py
```

6. Measure results in pgcli.

```sql
SELECT
	window_start,
	sum(total_tips) as tips_total
FROM output_q6
GROUP BY
	window_start
ORDER BY
	sum(total_tips) DESC
LIMIT 10;
```


## Submitting the solutions

- Form for submitting: [https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7)


## Learning in public

We encourage everyone to share what they learned. Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).
