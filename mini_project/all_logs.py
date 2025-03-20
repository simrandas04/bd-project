from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime
import json

def index_logs_to_elasticsearch(topic, es_host, max_messages=80):
    print(f"Processing topic: {topic}")
    
    # Connect to Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

    # Ensure the index name is in lowercase
    index_name = f"{topic.lower()}_logs"

    # Check if the index exists, and create it if it doesn't
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        print(f"Created index: {index_name}")

    # Kafka consumer for the given topic
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Consuming logs from topic: {topic}")

    # Process a limited number of messages
    for i, message in enumerate(consumer):
        if i >= max_messages:  # Stop after processing max_messages
            break

        log_data = message.value
        log_data['timestamp'] = datetime.now().isoformat()  # Add timestamp of when the log is indexed

        # Index the log data into Elasticsearch
        es.index(index=index_name, document=log_data)
        print(f"Indexed log into {index_name}: {log_data}")

    consumer.close()  # Close the consumer to free resources

if __name__ == "__main__":
    # Subscribe to multiple topics
    topics = ["INFO", "WARN", "ERROR"]
    for topic in topics:
        index_logs_to_elasticsearch(topic=topic, es_host="http://localhost:9200")
