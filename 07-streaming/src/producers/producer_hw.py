import pandas as pd
import time
import sys
from pathlib import Path
import json

# Allow running as `python src/consumers/consumer_hw.py` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.models_hw import Ride, ride_from_row, ride_serializer
from kafka import KafkaProducer


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [ \
    'lpep_pickup_datetime', \
    'lpep_dropoff_datetime', \
    'PULocationID', \
    'DOLocationID', \
    'passenger_count', \
    'trip_distance', \
    'tip_amount', \
    'total_amount' \
    ]

df = pd.read_parquet(url, columns=columns)
df = df.fillna(0)

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

topic_name = 'green-trips'


t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
#    time.sleep(0.01)
producer.flush()

"""
# After producer.flush(), send a fake far-future event to advance the watermark
from datetime import datetime
sentinel = Ride(
    lpep_pickup_datetime="2099-01-01 00:00:00",
    lpep_dropoff_datetime="2099-01-01 00:00:00",
    PULocationID=0,
    DOLocationID=0,
    passenger_count=0,
    trip_distance=0.0,
    tip_amount=0.0,
    total_amount=0.0
)
producer.send(topic_name, value=sentinel)
producer.flush()
"""

t1 = time.time()

print(f'took {(t1 - t0):.2f} seconds')
