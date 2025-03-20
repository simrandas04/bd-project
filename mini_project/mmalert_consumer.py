import logging
from kafka import KafkaConsumer
import json
import time

# Set up logging
logging.basicConfig(filename='alerts.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def log_alert(alert_message):
    logging.info(f"ALERT: {alert_message}")

def consume_logs():
    consumer = KafkaConsumer(
        'INFO', 'WARN', 'ERROR', 'REGISTRATION', 'HEARTBEAT',  # Added new topics
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Assuming logs are in JSON format
    )

    print("Listening for logs...")

    for message in consumer:
        log_data = message.value
        log_level = log_data['log_level']
        message_text = log_data['message']
        node_id = log_data.get('node_id', 'Unknown Node')  # Optional: handle missing node_id

        # Log specific message types
        if log_level == 'ERROR':
            log_alert(f"Critical ERROR detected: {message_text}")

        elif log_level == 'WARN':
            log_alert(f"Warning detected: {message_text}")

        elif log_level == 'REGISTRATION':
            log_alert(f"Node {node_id} registered successfully: {message_text}")

        elif log_level == 'HEARTBEAT':
            log_alert(f"Heartbeat received from {node_id}: {message_text}")

        # Additional alert conditions
        if 'timeout' in message_text or 'failed' in message_text:
            log_alert(f"ALERT: Possible failure detected in message: {message_text}")

if __name__ == "__main__":
    consume_logs()