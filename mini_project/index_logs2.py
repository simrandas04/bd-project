from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime
import json

def index_logs_to_elasticsearch(topic, es_host):
    # Connect to Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])  # Add scheme for HTTP

    # Ensure the index name is in lowercase
    index_name = f"{topic.lower()}_logs"  # Convert the topic name to lowercase

    # Check if the index exists, and create it if it doesn't
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        print(f"Created index: {index_name}")

    # Kafka consumer to consume logs from the Kafka topic
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",  # Kafka broker
        auto_offset_reset='earliest',  # Start from the earliest message in the topic
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
    )

    print(f"Indexing logs from topic: {topic} into Elasticsearch...")
    for message in consumer:
        log_data = message.value  # The log data is already a Python dictionary from JSON deserialization
        log_data['timestamp'] = datetime.now().isoformat()  # Add timestamp of when the log is indexed

        # Index the log data into Elasticsearch
        es.index(index=index_name, document=log_data)
        print(f"Indexed log: {log_data}")

if __name__ == "__main__":
    # Subscribe to multiple topics based on Fluentd output (INFO, WARN, ERROR)
    topics = ["INFO", "WARN", "ERROR"]
    for topic in topics:
        index_logs_to_elasticsearch(topic=topic, es_host="http://localhost:9200")
