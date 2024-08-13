import json
import requests
import logging
import sys, types

m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)


ITEM_LIST = ["BTC", "BNB", "SOL"]
FROM_NAME_TO_TOPIC = {"BTC": "btc-topic", "SOL": "sol-topic", "BNB": "bnb-topic"}


# Define a custom exception for handling HTTP errors
class HTTPException(Exception):
    pass


@retry(
    retry=retry_if_exception_type(
        (requests.exceptions.RequestException, HTTPException)
    ),
    wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff
    stop=stop_after_attempt(5),  # Retry up to 5 times
)
def fetch_data():
    response = requests.request(
        method="GET",
        url="http://api.coincap.io/v2/assets",
    )
    if response.status_code != 200:
        raise HTTPException(f"HTTP error: {response.status_code}")
    response = response.json()["data"]
    response = [
        item for item in response if item["symbol"] in FROM_NAME_TO_TOPIC.keys()
    ]
    return response


# Example usage
try:
    data = fetch_data()
    print(data)
except Exception as e:
    print(f"Failed to fetch data: {e}")


def send_data(**kwargs):

    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(0, 10),
    )

    messages = fetch_data()

    for msg in messages:
        logging.info(f"writing message {msg}")
        # logging.info(f"topic: {FROM_NAME_TO_TOPIC[msg["symbol"]]}")
        future = producer.send(FROM_NAME_TO_TOPIC[msg["symbol"]], {"message": msg})
        # future = producer.send("btc-topic", value=msg)

        # block fo synchronous sends
        try:
            metadata = future.get(timeout=10)

            logging.info(f"metadata: {metadata}")
        except KafkaError:
            pass

        logging.info(
            f"send msg successfully to {metadata.topic}.{metadata.partition}.{metadata.offset} "
        )

    producer.flush()
    producer.close()
