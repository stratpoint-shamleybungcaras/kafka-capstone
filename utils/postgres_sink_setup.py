import requests
import json

# IMPORT CONFIGURATIONS
from config.settings import KAFKA_CONNECT_URL, SILVER_TOPICS

def setup_postgres_sink():
    url = f"{KAFKA_CONNECT_URL}/connectors/postgres-sink-connector/config"
    silver_topics_str = ",".join(SILVER_TOPICS.values())
    
    config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        
        # Listen ONLY to the flattened silver topics
        "topics": silver_topics_str,
        
        "connection.url": "jdbc:postgresql://capstone-postgres:5432/kafka-capstone",
        "connection.user": "postgres",
        "connection.password": "postgres",
        
        "insert.mode": "insert",
        "auto.create": "true",
        "auto.evolve": "true",
        
        # Expect JSON from Faust, not Avro
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true"
    }

    headers = {'Content-Type': 'application/json'}
    response = requests.put(url, data=json.dumps(config), headers=headers)
    
    if response.status_code in [200, 201]:
        print("Success! Postgres Connector is updated and listening to silver topics.")
    else:
        print(f"Failed: {response.status_code} - {response.text}")

if __name__ == "__main__":
    setup_postgres_sink()