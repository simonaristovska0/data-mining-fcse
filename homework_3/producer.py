import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "health_data"
CSV_PATH = "data/online.csv"
MIN_DELAY = 0.5   # vo sekudni
MAX_DELAY = 1.5


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    security_protocol="PLAINTEXT"
)


df = pd.read_csv(CSV_PATH).drop(columns=["Diabetes_012"])

for record in df.to_dict(orient="records"):
    print(record)

    producer.send(TOPIC, value=record)

    sleep_time = random.uniform(MIN_DELAY, MAX_DELAY)
    print(f"Sleeping for {sleep_time} seconds\n")
    time.sleep(sleep_time)

producer.flush()
producer.close()







