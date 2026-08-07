import requests
import json
import psycopg2

# IMPORT CONFIGURATIONS
from config.settings import (
    KAFKA_CONNECT_URL,
    BRONZE_TOPICS, 
    SILVER_TOPICS
)

def setup_database():
    print("Connecting to PostgreSQL at localhost:5433...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="kafka-capstone",
            user="postgres",
            password="postgres"
        )
        
        conn.autocommit = True
        cursor = conn.cursor()

        print("Success: Database tables created and ready for streaming data!")
        
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to database: {e}")

def setup_postgres_sink():
    headers = {'Content-Type': 'application/json'}
    
    # Map each entity to its specific Primary Key field based on our Avro schemas
    primary_keys = {
        "users": "user_id",
        "products": "product_id",
        "orders": "order_id",
        "payments": "payment_id"
    }

    # Create a dedicated Upsert connector for each entity
    for entity, topic_name in SILVER_TOPICS.items():
        connector_name = f"postgres-sink-{entity}"
        url = f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config"
        
        pk_field = primary_keys.get(entity)
        
        config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": topic_name,
            
            "connection.url": "jdbc:postgresql://capstone-postgres:5432/kafka-capstone",
            "connection.user": "postgres",
            "connection.password": "postgres",
            
            # --- THE UPSERT CONFIGURATIONS ---
            "insert.mode": "upsert",
            "pk.mode": "record_value",
            "pk.fields": pk_field,
            
            "auto.create": "true",
            "auto.evolve": "true",
            
            # Expect JSON from Faust, not Avro
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true"
        }

        response = requests.put(url, data=json.dumps(config), headers=headers)
        
        if response.status_code in [200, 201]:
            print(f"Success! Upsert Connector '{connector_name}' is listening to '{topic_name}' (PK: {pk_field})")
        else:
            print(f"Failed to create {connector_name}: {response.status_code} - {response.text}")
def setup_mongo_sink():
    url = f"{KAFKA_CONNECT_URL}/connectors"
    bronze_topics_str = ",".join(BRONZE_TOPICS.values())
    
    payload = {
        "name": "mongodb-archival-sink",
        "config": {
            "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
            "tasks.max": "1",
            
            # Listen to all raw bronze topics
            "topics": bronze_topics_str,
            
            "connection.uri": "mongodb://capstone-mongo:27017",
            "database": "raw_events_archive",
            
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://capstone-schema-registry:8081"
        }
    }

    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print(response.text)

if __name__ == "__main__":
    setup_database()
    setup_postgres_sink()
    setup_mongo_sink()