#!/usr/bin/env python

"""
SCRIPT: streaming_data_reader.py
AUTHOR: IBM
DATE: 2022-09-21
DESCRIPTION: Streaming data consumer

AUDIT TRAIL START                               INIT  DATE
----------------------------------------------  ----- -----------
1. Initial version                              IBM   2022-09-21
2. Updated constant variables                   PR    2024-04-11

AUDIT TRAIL END
"""
import os
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
import sys, types
import json

m = types.ModuleType("kafka.vendor.six.moves", "Mock module")
setattr(m, "range", range)
sys.modules["kafka.vendor.six.moves"] = m

def consume_kafka_topic(topic, database, username, password, tablename):
    print("Connecting to the database")
    try:
        connection = psycopg2.connect(
            host="host.docker.internal",
            database=database,
            user=username,
            password=password,
        )
    except Exception:
        print("Could not connect to database. Please check credentials")
    else:
        print("Connected to database")
    cursor = connection.cursor()

    print("Connecting to Kafka")
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=["kafka:9092"],
            auto_offset_reset="latest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            api_version=(0, 10),
            max_poll_interval_ms=10,
            max_poll_records=5,
        )
    except Exception:
        print("Could not connect to Kafka")
    else:
        print("Connected to Kafka")
    print(f"Reading messages from the topic {topic}")

    for msg in consumer:
        # Extract information from kafka

        message = msg.value["message"]

        # Transform the date format to suit the database schema
        id = message["id"]
        rank = message["rank"]
        supply = round(float(message["supply"]),5)
        maxSupply = round(float(message["maxSupply"]),5) if not message["maxSupply"] is None else None
        marketCapUsd = round(float(message["marketCapUsd"]),5) if not message["marketCapUsd"] is None else None
        volumeUsd24Hr = round(float(message["volumeUsd24Hr"]),5) if not message["volumeUsd24Hr"] is None else None
        priceUsd = round(float(message["priceUsd"]),5) if not message["priceUsd"] is None else None
        changePercent24Hr = round(float(message["changePercent24Hr"]),5) if not message["changePercent24Hr"] is None else None
        vwap24Hr = round(float(message["vwap24Hr"]),5) if not message["vwap24Hr"] is None else None
        created_at = datetime.now().strftime("%D-%H:%M:%S")
        
        print(id,rank,supply,maxSupply,marketCapUsd,volumeUsd24Hr,priceUsd,changePercent24Hr,vwap24Hr,created_at)
        
        # Loading data into the database table
        sql = f"insert into {tablename}(rank,supply,maxSupply,marketCapUsd,volumeUsd24Hr,priceUsd,changePercent24Hr,vwap24Hr,created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        result = cursor.execute(
            sql,
            (
                rank,
                supply,
                maxSupply,
                marketCapUsd,
                volumeUsd24Hr,
                priceUsd,
                changePercent24Hr,
                vwap24Hr,
                created_at,
            ),
        )
        print(
            f"A record {id}-{priceUsd}-{created_at} was inserted into the {tablename}"
        )
        connection.commit()
        consumer.close()
    connection.close()
