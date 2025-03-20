import time
from fluent import sender
from datetime import datetime

def send_heartbeat(node_id, interval=60):
    logger = sender.FluentSender('node1', host='localhost', port=12345)

    while True:
        log_data = {
            "log_id": f"heartbeat-{int(time.time())}",
            "node_id": node_id,
            "log_level": "INFO",
            "message": "Heartbeat: System operational.",
            "timestamp": datetime.now().isoformat()
        }
        logger.emit("INFO", log_data)
        print(f"Heartbeat sent: {log_data}")
        time.sleep(interval)

if __name__ == "__main__":
    send_heartbeat(node_id="heartbeat_node")