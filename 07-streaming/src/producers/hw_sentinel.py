import json
from kafka import KafkaProducer
from dataclasses import asdict
from pathlib import Path
import sys

# Ensure your Ride class is imported or defined in the notebook
# from models_hw import Ride
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from models.models_hw import Ride, ride_from_row, ride_serializer

def send_manual_sentinel():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'], # Adjust if running inside Docker
        value_serializer=lambda v: json.dumps(asdict(v)).encode('utf-8')
    )
    
    topic_name = 'green-trips'

    # Create a ride from the far future
    # This timestamp must be > any timestamp in your dataset + 5 minutes
    sentinel_ride = Ride(
        lpep_pickup_datetime="2099-01-01 00:00:00",
        lpep_dropoff_datetime="2099-01-01 00:01:00",
        PULocationID=74,
        DOLocationID=1,
        passenger_count=1,
        trip_distance=0.0,
        tip_amount=0.0,
        total_amount=0.0
    )

    print(f"Sending sentinel to topic {topic_name}...")
    producer.send(topic_name, value=sentinel_ride)
    producer.flush()
    print("Sentinel sent! Check your Postgres sum(num_trips) now.")

# Execute
send_manual_sentinel()