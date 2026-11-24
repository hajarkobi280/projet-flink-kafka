from kafka import KafkaConsumer
import os

# Configuration
TOPIC = 'my-topic-test'
HDFS_PATH = '/user/hajar/meteo_history.json'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='hdfs-archiver'
)

print(f"📥 Archivage HDFS lancé... (Cible: {HDFS_PATH})")

# Création du dossier HDFS au début
os.system("hdfs dfs -mkdir -p /user/hajar")

try:
    for message in consumer:
        data = message.value.decode('utf-8')
        # On utilise appendToFile pour ajouter chaque ligne au fichier HDFS
        cmd = f"echo '{data}' | hdfs dfs -appendToFile - {HDFS_PATH}"
        os.system(cmd)
        print(f"💾 Donnée sauvegardée dans Hadoop")
except KeyboardInterrupt:
    print("🛑 Arrêt de l'archivage.")