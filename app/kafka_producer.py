from confluent_kafka import Producer
from .config import KAFKA_BOOTSTRAP_SERVERS

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}

producer = Producer(**producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def push_to_kafka(topic, message: str):
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.flush()
