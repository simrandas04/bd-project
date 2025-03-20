from kafka import KafkaConsumer
import time
import json

# Kafka consumer setup
consumer = KafkaConsumer(
    'node_heartbeat',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Track the latest heartbeat timestamps
heartbeat_tracker = {}

def monitor_heartbeats():
    for message in consumer:
        heartbeat = message.value
        node_id = heartbeat["node_id"]
        heartbeat_tracker[node_id] = time.time()
        print(f"Received heartbeat from {node_id} at {heartbeat['timestamp']}")

        # Check for unresponsive nodes
        current_time = time.time()
        for node, last_seen in list(heartbeat_tracker.items()):
            if current_time - last_seen > 20:  # Threshold: 20 seconds
                print(f"ALERT: Node {node} is unresponsive!")

if __name__ == "__main__":
    monitor_heartbeats()
