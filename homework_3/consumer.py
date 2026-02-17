from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "health_data_predicted"
CONSUMER_GROUP = "health_data_predictions_consumer"

consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8")
    )

try:
	for m in consumer:
		print(m.value)
finally:
	consumer.close()




