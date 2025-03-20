import time
import json
from kafka import KafkaConsumer
from threading import Thread

# Define a dictionary to store the last heartbeat time for each node
last_heartbeat = {}

# Set the timeout period for node failure detection
heartbeat_timeout = 15  # seconds

def monitor_heartbeats():
    consumer = KafkaConsumer(
        'heartbeats',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    for message in consumer:
        heartbeat = message.value
        node_id = heartbeat["node_id"]
        last_heartbeat[node_id] = time.time()  # Update last heartbeat time
        print(f"Received heartbeat from {node_id}")

def check_for_failures():
    while True:
        current_time = time.time()
        for node_id, last_time in list(last_heartbeat.items()):
            if current_time - last_time > heartbeat_timeout:
                print(f"ALERT: Node {node_id} has stopped sending heartbeats!")
                del last_heartbeat[node_id]  # Remove failed node from tracking
        
        time.sleep(5)  # Check every 5 seconds

# Run heartbeat monitoring and failure detection in separate threads
if __name__ == "__main__":
    Thread(target=monitor_heartbeats).start()
    Thread(target=check_for_failures).start()

