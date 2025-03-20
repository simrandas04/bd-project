from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime

def index_logs_to_elasticsearch(topic, es_host):
    # Connect to Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    index_name = "node1_logs"

    # Check if the index exists, and create it if it doesn't
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        print(f"Created index: {index_name}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print(f"Indexing logs from topic: {topic} into Elasticsearch...")
    for message in consumer:
        log_data = {
            "log_message": message.value,
            "timestamp": datetime.now().isoformat()
        }
        es.index(index=index_name, document=log_data)
        print(f"Indexed log: {log_data}")

if __name__ == "__main__":
    index_logs_to_elasticsearch(topic="node1_logs", es_host="http://localhost:9200")
