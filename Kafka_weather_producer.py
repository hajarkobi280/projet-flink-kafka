import requests
import json
import time
from kafka import KafkaProducer

# Configuration du Producer Kafka
# On s'assure que les données sont envoyées au format JSON UTF-8
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Nom du topic (doit être le même que dans ton code Flink)
TOPIC_NAME = 'my-topic-test'

# Coordonnées GPS de Casablanca
LAT = 33.5731
LON = -7.5898

def get_weather_data():
    """Récupère les données météo réelles via API sans clé requise"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true"
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        print(f"❌ Erreur lors de l'appel API : {e}")
        return None

print("🚀 Producteur Big Data démarré...")
print(f"📡 Envoi des données météo de Casablanca vers le topic : {TOPIC_NAME}")

try:
    while True:
        data = get_weather_data()
        
        if data and 'current_weather' in data:
            current = data['current_weather']
            
            # On crée un dictionnaire propre pour Kafka
            payload = {
                "city": "Casablanca",
                "temperature": current.get('temperature'),
                "windspeed": current.get('windspeed'),
                "weathercode": current.get('weathercode'),
                "timestamp": time.time()
            }
            
            # Envoi vers Kafka
            producer.send(TOPIC_NAME, value=payload)
            print(f"✅ Donnée envoyée à {time.strftime('%H:%M:%S')} : {payload}")
        
        # On attend 5 secondes avant la prochaine récupération
        # (L'API gratuite a des limites si on appelle trop vite)
        time.sleep(5)

except KeyboardInterrupt:
    print("\n🛑 Arrêt du producteur par l'utilisateur.")
except Exception as e:
    print(f"❌ Erreur critique : {e}")
finally:
    producer.close()