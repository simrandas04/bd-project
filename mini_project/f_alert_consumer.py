import logging
from kafka import KafkaConsumer
import json
import time

# Set up logging
logging.basicConfig(filename='alerts.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def log_alert(alert_message):
    logging.info(alert_message)

def consume_logs():
    consumer = KafkaConsumer(
        'HEARTBEAT', 'WARN', 'ERROR', 'REGISTRATION',  # Topics to listen to
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Assuming logs are in JSON format
    )

    print("Listening for logs...")

    # A dictionary to track the last heartbeat timestamp for each node
    node_last_heartbeat = {}
    heartbeat_timeout = 30  # 30 seconds threshold for node down alert

    for message in consumer:
        log_data = message.value
        log_topic = message.topic  # Get the topic of the message
        timestamp = log_data.get('timestamp', 'N/A')  # Default to N/A if not present

        # Handle HEARTBEAT topic
        if log_topic == 'HEARTBEAT':
            node_id = log_data.get('node_id')
            status = log_data.get('status', 'N/A')

            # Log the heartbeat message
            log_alert(f"Topic: {log_topic}, Timestamp: {timestamp}, Data: {log_data}")
            print(f"[{timestamp}] {log_topic}: {log_data}")

            # Update the last heartbeat timestamp for the node
            node_last_heartbeat[node_id] = timestamp

        # Handle REGISTRATION topic
        elif log_topic == 'REGISTRATION':
            log_alert(f"Topic: {log_topic}, Timestamp: {timestamp}, Data: {log_data}")
            print(f"[{timestamp}] {log_topic}: {log_data}")

        # Handle WARN and ERROR topics
        elif log_topic == 'WARN' or log_topic == 'ERROR':
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

        # Check for node heartbeat timeouts (if the node hasn't sent a heartbeat within the last 30 seconds)
        current_time = time.time()  # Get the current time in seconds
        for node_id, last_heartbeat in list(node_last_heartbeat.items()):
            try:
                # Convert the timestamp string to a timestamp object for comparison
                last_heartbeat_time = time.mktime(time.strptime(last_heartbeat, '%Y-%m-%dT%H:%M:%S.%f'))
                time_difference = current_time - last_heartbeat_time

                # If the difference exceeds the timeout threshold, mark the node as DOWN
                if time_difference > heartbeat_timeout:
                    # Log the node's status as DOWN
                    log_alert(f"ALERT: Node {node_id} has not sent a heartbeat for {int(time_difference)} seconds. Marking as DOWN.")
                    print(f"[{last_heartbeat}] HEARTBEAT: {{'node_id': '{node_id}', 'message_type': 'HEARTBEAT', 'status': 'DOWN', 'timestamp': '{last_heartbeat}'}}")

            except Exception as e:
                print(f"Error processing heartbeat timestamp: {e}")

if __name__ == "__main__":
    consume_logs()
