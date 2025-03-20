import logging
from kafka import KafkaConsumer
import json

# Set up logging
logging.basicConfig(filename='alerts.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def log_alert(alert_message):
    logging.info(alert_message)

def consume_logs():
    consumer = KafkaConsumer(
        'HEARTBEAT', 'WARN', 'ERROR', 'REGISTRATION',  # Topics to listen to
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Assuming logs are in JSON format
    )

    print("Listening for logs...")

    for message in consumer:
        log_data = message.value
        log_topic = message.topic  # Get the topic of the message
        timestamp = log_data.get('timestamp', 'N/A')  # Default to N/A if not present

        # Handle HEARTBEAT and REGISTRATION differently, as they might not include log_level
        if log_topic in ['HEARTBEAT', 'REGISTRATION']:
            log_alert(f"Topic: {log_topic}, Timestamp: {timestamp}, Data: {log_data}")
            print(f"[{timestamp}] {log_topic}: {log_data}")
        else:
            # Handle WARN and ERROR
            log_level = log_data.get('log_level', 'UNKNOWN')  # Default to UNKNOWN if not present
            message_text = log_data.get('message', '')
            print(f"[{timestamp}] {log_topic} - {log_level}: {message_text}")
            log_alert(f"Topic: {log_topic}, Timestamp: {timestamp}, Level: {log_level}, Message: {message_text}")

            # Additional conditions for WARN and ERROR
            if log_level == 'ERROR':
                log_alert(f"Critical ERROR detected: {message_text}")
            elif log_level == 'WARN':
                log_alert(f"Warning detected: {message_text}")
            if 'timeout' in message_text.lower() or 'failed' in message_text.lower():
                log_alert(f"ALERT: Possible failure detected in message: {message_text}")

if _name_ == "_main_":
    consume_logs()