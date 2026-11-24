from pyflink.table import EnvironmentSettings, TableEnvironment
import os

# Configuration de l'environnement Flink
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)

# Chargement du connecteur Kafka que vous avez téléchargé
# On utilise le chemin absolu pour éviter les erreurs
dir_path = os.path.dirname(os.path.realpath(__file__))
jar_path = f"file://{dir_path}/connectors/flink-sql-connector-kafka-3.0.1-1.18.jar"
t_env.get_config().set("pipeline.jars", jar_path)

# Définition de la source Kafka (Data Ingestion)
t_env.execute_sql("""
    CREATE TABLE kafka_tweets (
        user_name STRING,
        tweet_content STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'twitter_data',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'hajar-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# Affichage en temps réel (Stream Processing)
print("🚀 Flink est en attente de données Kafka...")
t_env.execute_sql("SELECT user_name, tweet_content FROM kafka_tweets").print()
# Dans processeur_tweets.py
import os
from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)

# On pointe vers le dossier lib où on a mis nos JARs
dir_path = "/home/hajar/mon_projet_flink/flink-1.18.0/lib"
kafka_connector = f"file://{dir_path}/flink-sql-connector-kafka-3.0.1-1.18.jar"
kafka_clients = f"file://{dir_path}/kafka-clients-3.4.0.jar"

# On charge les deux !
t_env.get_config().set("pipeline.jars", f"{kafka_connector};{kafka_clients}")