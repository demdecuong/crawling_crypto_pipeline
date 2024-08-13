from kafka import KafkaConsumer
import json
import logging

# Set up logging to help with troubleshooting
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    consumer = KafkaConsumer('sol-topic', bootstrap_servers=["localhost:9094"],
                             auto_offset_reset="latest",
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                             api_version=(0, 10),
                             max_poll_interval_ms=100)

    logger.info("Kafka Consumer started. Listening to 'sol-topic'...")

    for msg in consumer:
        print(msg.value)

except Exception as e:
    logger.error(f"An error occurred: {e}")
