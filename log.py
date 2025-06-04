import os
import json
import random
import time
from datetime import datetime

OUTPUT_DIR = "data\streaming_logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

start_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
file_path = os.path.join(OUTPUT_DIR, f"logs_{start_time}.json")

user_agents = [
    "Mozilla/5.0", "Chrome/90.0", "Safari/537.36", "Edge/91.0"
]
ips = [f"192.168.0.{i}" for i in range(1, 10)]

try:
    print(f"[INFO] Writing to file: {file_path}")
    print("[INFO] Starting log generation. Press Ctrl+C to stop.")

    while True:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ip = random.choice(ips)
        agent = random.choice(user_agents)

        log_entry = {
            "timestamp": timestamp,
            "ip": ip,
            "user_agent": agent
        }

        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")

        print("Written:", log_entry)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n[INFO] Log generation stopped by user.")
