import argparse
import json
import time
import random
import logging
from datetime import datetime
from threading import Thread
from fluent import sender
from fluent import event

# Fluentd logger
logger = sender.FluentSender('node1', host='localhost', port=12345)

def log_event(log_data):
    if logger.emit(log_data['log_level'], log_data):
        print(f"Log sent successfully: {log_data['log_level']}: {log_data['message']}")
    else:
        print(f"Failed to send log: {log_data['log_level']}: {log_data['message']}")

def send_registration(node_id, service_name):
    """Send a one-time registration event."""
    registration_data = {
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": service_name,
        "timestamp": datetime.now().isoformat()
    }
    if logger.emit("REGISTRATION", registration_data):
        print("Registration sent:", registration_data)
    else:
        print("Failed to send registration.")

def send_heartbeat(node_id):
    """Send periodic heartbeat events."""
    while True:
        heartbeat_data = {
            "node_id": node_id,
            "message_type": "HEARTBEAT",
            "status": "UP",
            "timestamp": datetime.now().isoformat()
        }
        if logger.emit("HEARTBEAT", heartbeat_data):
            print("Heartbeat sent:", heartbeat_data)
        else:
            print("Failed to send heartbeat.")
        time.sleep(10)  # Send heartbeat every 10 seconds

def generate_logs(node_id, service_name):
    """Generate logs of different types."""
    while True:
        log_type = random.choices(['INFO', 'WARN', 'ERROR'], weights=[70, 20, 10], k=1)[0]
        # INFO logs 70% of the time, WARN 20% of the time, ERROR 10% of the time

        if log_type == 'INFO':
            bus_id = random.randint(100, 999)
            stop_id = random.randint(100, 999)
            log_data = {
                "log_id": f"{node_id}-{int(time.time())}",
                "node_id": node_id,
                "log_level": "INFO",
                "message_type": "LOG",
                "message": f"Bus ID {bus_id} arrived at Stop ID {stop_id} at {datetime.now().strftime('%I:%M %p')}",
                "service_name": service_name,
                "timestamp": datetime.now().isoformat()
            }
        elif log_type == 'WARN':
            route_id = random.randint(100, 999)
            delay_time = random.randint(5, 30)
            log_data = {
                "log_id": f"{node_id}-{int(time.time())}",
                "node_id": node_id,
                "log_level": "WARN",
                "message_type": "LOG",
                "message": f"Bus delay for Route ID {route_id} expected to be {delay_time} minutes",
                "service_name": service_name,
                "timestamp": datetime.now().isoformat()
            }
        elif log_type == 'ERROR':
            passenger_id = random.randint(1000, 9999)
            log_data = {
                "log_id": f"{node_id}-{int(time.time())}",
                "node_id": node_id,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": f"Ticket purchase failed for Passenger ID {passenger_id} due to payment processing error",
                "service_name": service_name,
                "timestamp": datetime.now().isoformat()
            }

        log_event(log_data)

        # Sleep for a random interval between 2 and 5 seconds
        time.sleep(random.randint(2, 5))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id", required=True, help="Unique identifier for the node")
    parser.add_argument("--service_name", required=True, help="Name of the service")
    args = parser.parse_args()

    # Send registration message
    send_registration(args.node_id, args.service_name)

    # Start heartbeat thread
    heartbeat_thread = Thread(target=send_heartbeat, args=(args.node_id,))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    # Generate logs
    generate_logs(args.node_id, args.service_name)