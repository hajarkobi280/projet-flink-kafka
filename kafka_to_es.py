from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import time

es = Elasticsearch(['http://localhost:9200'])

consumer = KafkaConsumer(
    'my-topic-test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🚀 Envoi vers Elasticsearch lancé (en attente de données)...")

for message in consumer:
    try:
        data = message.value
        # On ajoute un timestamp pour Kibana
        data['@timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # On indexe toute la donnée, peu importe les noms des colonnes
        es.index(index="meteo-casablanca", document=data)
        
        # On affiche ce qu'on a reçu pour débugger
        print(f"✅ Donnée indexée : {data}")
        
    except Exception as e:
        print(f"❌ Erreur : {e}")