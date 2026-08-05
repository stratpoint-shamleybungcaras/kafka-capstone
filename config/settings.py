# Configuration file for the Capstone Project

import os
from dotenv import load_dotenv

# Project Path Files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_DIR = os.path.join(BASE_DIR, 'schema')
# Load environment variables from .env file
load_dotenv()  
# Kafka & Security Configuration (Loaded dynamically from .env)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9093")
KAFKA_USER = os.getenv("KAFKA_ADMIN_USER", "admin")
KAFKA_PASSWORD = os.getenv("KAFKA_ADMIN_PASS", "admin-secret")

# Librdkafka & Faust Security Configurations
SASL_CONFIG = {
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': KAFKA_USER,
    'sasl.password': KAFKA_PASSWORD,
}

# External Service URLs
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085")
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8086")
FAUST_BROKER_URL = f"kafka://{KAFKA_BROKER}"

# Topic Names (Raw Data)
BRONZE_TOPICS = {   
    "users": "user-bronze",
    "products": "product-bronze",
    "orders": "order-bronze",
    "payments": "payment-bronze"
}

# Topic Names (Cleaned/Flattened Data)
SILVER_TOPICS = {
    "users": "user-silver",
    "products": "product-silver",
    "orders": "order-silver",
    "payments": "payment-silver"
}

DLQ_TOPICS = {
    "users": "user-dlq",
    "products": "product-dlq",
    "orders": "order-dlq",
    "payments": "payment-dlq"
}
