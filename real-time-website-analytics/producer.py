from kafka import KafkaProducer
import json
import random
from datetime import datetime, timezone, timedelta

# Create the Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v:json.dumps(v).encode('utf-8')
)

# Declare payload data
user_ids = [1, 2, 3, 4, 5]
event_types = ["play", "pause"]
delay_seconds = [10, 15, 20, 30, 40]

for count in range(5):
    user_id = random.choice(user_ids)
    event_type = random.choice(event_types)
    value = random.randint(1, 10)
    
    now = datetime.now(timezone.utc)
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    # event_time = (now - timedelta(seconds=random.choice(delay_seconds))).strftime(date_format)
    event_time = now.strftime(date_format)
    
    payload = { "user_id": user_id, "event_time": event_time, "value": value, "event_type": event_type }
    print("Sending: ", payload)
    producer.send("user_events", payload)
    
producer.flush()