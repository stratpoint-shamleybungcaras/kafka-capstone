import os 
import sys
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# IMPORT YOUR CENTRALIZED CONFIGURATION
from config.settings import (
    KAFKA_BROKER, 
    SCHEMA_REGISTRY_URL, 
    BRONZE_TOPICS, 
    SILVER_TOPICS, 
    DLQ_TOPICS, 
    SASL_CONFIG
    )

class SchemaManager:
    """
    A centralized manager for interacting with the Confluent Schema Registry
    and provisioning Kafka topics with custom partitions and retention policies.
    """
    def __init__(self, registry_url=SCHEMA_REGISTRY_URL, broker_url=KAFKA_BROKER):
        self.schema_registry_url = registry_url
        self.broker_url = broker_url
        self.client = self.get_schema_registry_client()

        # Merge the broker URL with the SASL dictionary
        admin_config = {'bootstrap.servers': self.broker_url}
        admin_config.update(SASL_CONFIG)

        self.admin_client = AdminClient(admin_config)
    
    def provision_topics(self, topic_names: list, num_partitions: int = 3):
        print(f"Checking broker configuration at {self.broker_url}...")
        
        metadata = self.admin_client.list_topics(timeout=5)
        existing_topics = metadata.topics.keys()
        
        new_topics_to_create = []
        topic_config = {
            "cleanup.policy": "delete",
            "retention.ms": "604800000", 
            "segment.ms": "604800000"
        }
        
        for topic in topic_names:
            if topic in existing_topics:
                actual_partitions = len(metadata.topics[topic].partitions)
                print(f"Topic '{topic}' already exists with {actual_partitions} partition(s).")
            else:
                print(f"Queueing creation: '{topic}' with {num_partitions} partitions and 7-day retention...")
                new_topics_to_create.append(
                    NewTopic(
                        topic, 
                        num_partitions=num_partitions, 
                        replication_factor=1,
                        config=topic_config
                    )
                )
        
        if new_topics_to_create:
            fs = self.admin_client.create_topics(new_topics_to_create)
            for topic, future in fs.items():
                try:
                    future.result() 
                    print(f"Success: Provisioned Topic '{topic}' with {num_partitions} partitions!")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
        else:
            print("All topics are already accounted for on the cluster.")

    def get_schema_registry_client(self) -> SchemaRegistryClient:
        config = {'url': self.schema_registry_url}
        sr_username = os.getenv("SCHEMA_REGISTRY_USERNAME", "")
        sr_password = os.getenv("SCHEMA_REGISTRY_PASSWORD", "")

        if sr_username and sr_password:
            config.update({
                'basic.auth.credentials.source': 'USER_INFO',
                'basic.auth.user.info': f"{sr_username}:{sr_password}",
            })
        return SchemaRegistryClient(config)

    def register_schema_from_file(self, topic_name: str, file_path: str) -> int:
        if not os.path.exists(file_path):
            print(f"Skipping {topic_name}: File not found at {file_path}")
            return None

        with open(file_path, "r") as file:
            schema_string = file.read()

        avro_schema = Schema(schema_string, schema_type="AVRO")
        subject_name = f"{topic_name}-value"

        try:
            schema_id = self.client.register_schema(subject_name, avro_schema)
            print(f"Success: Registered Subject '{subject_name}' | ID: {schema_id}")
            return schema_id
        except Exception as e:
            print(f"Error: Failed to register {topic_name}: {e}")
            return None


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    SCHEMA_DIR = os.path.join(BASE_DIR, "..", "schema")

    # Map Schemas to their respective Bronze Topics
    schemas_to_upload = {
        BRONZE_TOPICS["users"]: os.path.join(SCHEMA_DIR, "user_events.avsc"),
        BRONZE_TOPICS["products"]: os.path.join(SCHEMA_DIR, "product_events.avsc"),
        BRONZE_TOPICS["orders"]: os.path.join(SCHEMA_DIR, "order_events.avsc"),
        BRONZE_TOPICS["payments"]: os.path.join(SCHEMA_DIR, "payment_events.avsc")
    }

    # 3. COLLECT ALL TOPICS (Bronze + Silver + DLQ) TO PROVISION
    all_topics_to_create = (
        list(BRONZE_TOPICS.values()) + 
        list(SILVER_TOPICS.values()) + 
        list(DLQ_TOPICS.values())
    )

    print("Initializing Capstone Cluster Manager...\n")
    manager = SchemaManager()
    
    print("--- Phase 1: Broker Infrastructure Initialization ---")
    manager.provision_topics(all_topics_to_create, num_partitions=3)
    
    print("\n--- Phase 2: Schema Registry Registration ---")
    for topic, path in schemas_to_upload.items():
        manager.register_schema_from_file(topic, path)