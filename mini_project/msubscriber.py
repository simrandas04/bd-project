from kafka import KafkaConsumer
import argparse

def consume_logs():
    consumer = KafkaConsumer(
        "INFO",
        bootstrap_servers="localhost:9092",
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    #print(f"Subscribed to topic: {topic}. Listening for logs...")
    for message in consumer:
        print(message.value)

if __name__ == "__main__":
    '''parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help="Kafka topic to consume logs from")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka bootstrap server address")
    args = parser.parse_args()'''

    consume_logs()