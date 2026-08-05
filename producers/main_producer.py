import time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# IMPORT CENTRALIZED CONFIGURATION
from config.settings import KAFKA_BROKER, SCHEMA_REGISTRY_URL, BRONZE_TOPICS, SASL_CONFIG
from utils.dummy_data_generator import EcommerceDataGenerator

def delivery_report(err, msg):
    """Callback triggered on successful or failed delivery to Kafka."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} | Partition: {msg.partition()}")

def create_serializers(schema_registry_client, topic_names):
    """Fetches schemas and builds AvroSerializers for our configured topics."""
    serializers = {}
    for topic in topic_names:
        subject_name = f"{topic}-value"
        latest_schema = schema_registry_client.get_latest_version(subject_name)
        
        serializers[topic] = AvroSerializer(
            schema_registry_client,
            latest_schema.schema.schema_str,
            to_dict=lambda custom_dict, ctx: custom_dict
        )
        print(f"Loaded schema for {topic} (Version {latest_schema.version})")
    return serializers

def main():
    print("Initializing Kafka Capstone Producer...")
    
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    # Extract the raw bronze topic names
    pipeline_topics = list(BRONZE_TOPICS.values())
    serializers = create_serializers(sr_client, pipeline_topics)

    # SECURE PRODUCER CONFIGURATION
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'capstone-master-producer',
        **SASL_CONFIG,                                # Injects your credentials
        'enable.idempotence': True,
        'acks': 'all',
        'max.in.flight.requests.per.connection': 1,
        'broker.address.family': 'v4',                # Fixes the MacOS Localhost socket drop
    }
    
    producer = Producer(producer_config)
    data_engine = EcommerceDataGenerator()

    print("\nStarting Live Data Stream... (Press Ctrl+C to stop)")
    try:
        while True:
            # Step A: Generate the raw data
            user_data = data_engine.generate_user()
            product_data = data_engine.generate_product()
            order_data = data_engine.generate_order()
            payment_data = data_engine.generate_payment(
                order_id=order_data["order_id"], 
                amount=order_data["total_amount"]
            )

            # Step B: Map data using the BRONZE_TOPICS dictionary
            events_to_send = [
                (BRONZE_TOPICS["users"], user_data["user_id"], user_data),
                (BRONZE_TOPICS["products"], product_data["product_id"], product_data),
                (BRONZE_TOPICS["orders"], order_data["order_id"], order_data),
                (BRONZE_TOPICS["payments"], payment_data["payment_id"], payment_data)
            ]

            # Step C: Serialize and push to Kafka
            for topic, key, payload in events_to_send:
                producer.produce(
                    topic=topic,
                    key=str(key), # Ensure key is a string
                    value=serializers[topic](
                        payload, 
                        SerializationContext(topic, MessageField.VALUE)
                    ),
                    on_delivery=delivery_report
                )
            
            producer.poll(0)
            time.sleep(2) 

    except KeyboardInterrupt:
        print("\nShutting down producer..")
    finally:
        producer.flush()
        print("Producer closed.")

if __name__ == "__main__":
    main()