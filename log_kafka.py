import os
from datetime import datetime
import time
from kafka import KafkaProducer
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092')
user_agents = [
    "Mozilla/5.0", "Chrome/90.0", "Safari/537.36", "Edge/91.0"
]
ips = [f"192.168.0.{i}" for i in range(1, 10)]

while True:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ip = random.choice(ips)
    agent = random.choice(user_agents)
    message = f"{timestamp},{ip},{agent}"
    producer.send('logs_topic', message.encode('utf-8'))
    print("Sent:", message)
    time.sleep(1) 
