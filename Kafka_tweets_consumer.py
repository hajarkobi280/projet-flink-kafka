import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'my-topic-test',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("✅ Consumer démarré...")

for message in consumer:
    try:
        tweet = json.loads(message.value)  # 👈 conversion ici
        print(tweet)
    except Exception as e:
        print("⚠️ Message ignoré (non JSON):", message.value)