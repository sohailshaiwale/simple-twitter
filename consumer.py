from kafka import KafkaConsumer
import json

topic_name = 'twitter'

consumer = KafkaConsumer(topic_name,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for message in consumer:
    message = message.value;
    print(message)


