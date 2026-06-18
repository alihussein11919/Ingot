from kafka import KafkaProducer
from kafka.serializer import Serializer
import json

class JsonSerializer(Serializer):
    def serialize(self, topic, data):
        return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=JsonSerializer()
)

def publish(topic, message):
    producer.send(topic, message)
    producer.flush()