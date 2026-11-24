import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

def start_flink_analysis():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 1. AJOUT DES JARS (Vérifie bien tes chemins)
    lib_dir = "/home/hajar/mon_projet_flink/flink-1.18.0/lib"
    env.add_jars(f"file://{lib_dir}/flink-sql-connector-kafka-3.0.1-1.18.jar")
    env.add_jars(f"file://{lib_dir}/kafka-clients-3.4.0.jar")

    # 2. SOURCE KAFKA
    properties = {"bootstrap.servers": "localhost:9092", "group.id": "hajar_final_group"}
    consumer = FlinkKafkaConsumer("my-topic-test", SimpleStringSchema(), properties)
    # On commence par la fin pour éviter les vieux messages corrompus
    consumer.set_start_from_latest() 

    ds = env.add_source(consumer)

    # 3. ANALYTICS (Filtrage et Transformation)
    def process_weather(json_str):
        try:
            data = json.loads(json_str)
            temp = data['temperature']
            status = "🔥 CHAUD" if temp > 20 else "❄️ FRAIS"
            return f"📊 [ANALYTICS] Ville: {data['city']} | Temp: {temp}°C | État: {status}"
        except:
            return None

    # On filtre les erreurs et on affiche
    ds.map(process_weather).filter(lambda x: x is not None).print()

    env.execute("Flink Real-Time Weather Analysis")

if __name__ == '__main__':
    start_flink_analysis()