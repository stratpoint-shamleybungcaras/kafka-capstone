import json
from confluent_kafka import DeserializingConsumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# IMPORT CENTRALIZED CONFIGURATION
from config.settings import (
    KAFKA_BROKER, 
    SCHEMA_REGISTRY_URL, 
    BRONZE_TOPICS, 
    SASL_CONFIG, 
    DLQ_TOPICS
)

def get_consumer_configs(group_id: str = "kafka-capstone-group"):
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)
    string_deserializer = StringDeserializer()
    
    config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": group_id,
        "key.deserializer": string_deserializer,
        "value.deserializer": avro_deserializer,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        **SASL_CONFIG  # Inject security credentials
    }
    return config

def get_dlq_producer():
    dlq_config = {
        "bootstrap.servers": KAFKA_BROKER,
        **SASL_CONFIG  # Inject security credentials
    }
    return Producer(dlq_config)


def run_consumer(topics: list):
    consumer = DeserializingConsumer(get_consumer_configs())
    dlq_producer = get_dlq_producer()
    
    # DYNAMIC MAPPING: Matches e.g., 'user-bronze' -> 'user-dlq'
    topic_to_dlq = {BRONZE_TOPICS[key]: DLQ_TOPICS.get(key, "general-dlq") for key in BRONZE_TOPICS}
    
    consumer.subscribe(topics)
    print(f"Subscribed to topics: {topics}")
    print("Listening for messages... (Manual Commits Active). Press Ctrl+C to stop.")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
                
            topic = msg.topic()
            key = msg.key()
            value = msg.value()
            
            try:
                # 1. Process the message (For now, just reading it)
                print(f"Processed from {topic}: {value.get('order_id') or value.get('payment_id') or value.get('user_id') or value.get('product_id')}")
                
                # 2. Manually commit the offset ONLY after successful processing
                consumer.commit(asynchronous=False)
                
            except Exception as e:
                # Determine the correct entity DLQ topic dynamically
                target_dlq_topic = topic_to_dlq.get(topic, "general-dlq")
                
                print(f"Error processing message on {topic}! Routing to {target_dlq_topic}... Reason: {e}")
                
                # 3. Dead Letter Queue (DLQ)
                dlq_payload = json.dumps({"error": str(e), "failed_value": str(value)})
                dlq_producer.produce(
                    topic=target_dlq_topic,  
                    key=str(key),
                    value=dlq_payload
                )
                dlq_producer.poll(0)
                
                # We still manually commit here so the consumer doesn't get stuck
                consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("\n Shutting down consumer...")
    finally:
        consumer.close()
        dlq_producer.flush()
        print("Kafka connections securely closed.")

if __name__ == "__main__":
    # Dynamically grab ALL values from the BRONZE_TOPICS dictionary
    target_topics = list(BRONZE_TOPICS.values())
    
    run_consumer(target_topics)