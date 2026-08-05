import requests
import json

# IMPORT CONFIGURATIONS
from config.settings import KAFKA_CONNECT_URL, BRONZE_TOPICS

def create_mongo_sink():
    url = f"{KAFKA_CONNECT_URL}/connectors"
    
    # 2DYNAMICALLY JOIN THE BRONZE TOPICS INTO A COMMA-SEPARATED STRING
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
    create_mongo_sink()