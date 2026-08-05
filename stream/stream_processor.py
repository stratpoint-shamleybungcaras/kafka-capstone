import faust
import struct
import io
import requests
import json
import os
from fastavro import parse_schema, schemaless_reader
from faust.auth import SASLCredentials

# Load dotenv if it's not already being loaded by settings.py
from dotenv import load_dotenv
load_dotenv()

# IMPORT CENTRALIZED CONFIGURATION
from config.settings import (
    FAUST_BROKER_URL, 
    SCHEMA_REGISTRY_URL, 
    BRONZE_TOPICS, 
    SILVER_TOPICS
)

# SECURE FAUST CONFIGURATION (No hardcoded passwords!)
app = faust.App(
    'silver-topics-flattener-v1', 
    broker=FAUST_BROKER_URL,
    broker_credentials=SASLCredentials(
        username=os.environ["KAFKA_ADMIN_USER"], 
        password=os.environ["KAFKA_ADMIN_PASS"],
        mechanism='PLAIN'
    ),
    consumer_auto_offset_reset='earliest'
)

# 1. AVRO DECODER
schema_cache = {}

def decode_confluent_avro(raw_bytes):
    """Strips the 5-byte Confluent header and decodes the Avro payload."""
    if not raw_bytes:
        return None
        
    magic, schema_id = struct.unpack('>bI', raw_bytes[:5])
    
    if schema_id not in schema_cache:
        # USE THE CONFIG VARIABLE FOR THE REGISTRY
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}")
        schema_str = response.json().get("schema")
        schema_cache[schema_id] = parse_schema(json.loads(schema_str))
        
    bytes_reader = io.BytesIO(raw_bytes[5:])
    return schemaless_reader(bytes_reader, schema_cache[schema_id])

# 2. TOPIC DEFINITIONS (Using Config Maps)
def create_topic_trio(entity):
    """Returns a tuple of (bronze, silver, dlq) topics for a given entity."""
    bronze = app.topic(BRONZE_TOPICS[entity], value_serializer='raw')
    silver = app.topic(SILVER_TOPICS[entity])
    dlq    = app.topic(DLQ_TOPICS[entity], value_serializer='raw')
    return bronze, silver, dlq

# Unpack topics cleanly
orders_topic, orders_silver_topic, orders_dlq_topic = create_topic_trio('orders')
payments_topic, payments_silver_topic, payments_dlq_topic = create_topic_trio('payments')
product_topic, products_silver_topic, products_dlq_topic = create_topic_trio('products')
users_topic, users_silver_topic, users_dlq_topic = create_topic_trio('users')


# 3. SCHEMA ENVELOPES FOR POSTGRES
user_schema = {
    "type": "struct", "name": "users_silver",  # Updated to match silver naming
    "fields": [
        {"type": "string", "optional": True, "field": "user_id"},
        {"type": "string", "optional": True, "field": "email"},
        {"type": "boolean", "optional": True, "field": "is_active"},
        {"type": "boolean", "optional": True, "field": "pref_email_opt_in"},
        {"type": "string", "optional": True, "field": "pref_preferred_currency"},
        {"type": "int64", "optional": True, "field": "created_at"}
    ]
}

product_schema = {
    "type": "struct", "name": "products_silver", # Updated to match silver naming
    "fields": [
        {"type": "string", "optional": True, "field": "product_id"},
        {"type": "string", "optional": True, "field": "name"},
        {"type": "double", "optional": True, "field": "price"},
        {"type": "string", "optional": True, "field": "tags"},
        {"type": "boolean", "optional": True, "field": "is_digital_download"}
    ]
}

order_schema = {
    "type": "struct", "name": "orders_silver", # Updated to match silver naming
    "fields": [
        {"type": "string", "optional": True, "field": "order_id"},
        {"type": "string", "optional": True, "field": "user_id"},
        {"type": "double", "optional": True, "field": "total_amount"},
        {"type": "boolean", "optional": True, "field": "discount_applied"},
        {"type": "int64", "optional": True, "field": "order_timestamp"}
    ]
}

payment_schema = {
    "type": "struct", "name": "payments_silver", # Updated to match silver naming
    "fields": [
        {"type": "string", "optional": True, "field": "payment_id"},
        {"type": "string", "optional": True, "field": "order_id"},
        {"type": "double", "optional": True, "field": "amount"},
        {"type": "string", "optional": True, "field": "status"},
        {"type": "double", "optional": True, "field": "risk_score"},
        {"type": "string", "optional": True, "field": "card_network"},
        {"type": "int64", "optional": True, "field": "timestamp"}
    ]
}

# ==========================================
# 4. AGENTS
# ==========================================

@app.agent(users_topic)
async def process_users(stream):
    async for raw_bytes in stream:
        print(f"DEBUG [USERS]: Received bytes...")
        try:
            user = decode_confluent_avro(raw_bytes)
            if not user: continue
            
            prefs = user.get("preferences", {})
            silver_user = {
                "user_id": user.get("user_id"),
                "email": user.get("email"),
                "is_active": user.get("is_active"),
                "pref_email_opt_in": prefs.get("email_opt_in", False),
                "pref_preferred_currency": prefs.get("preferred_currency", "USD"),
                "created_at": user.get("created_at")
            }
            await users_silver_topic.send(value={"schema": user_schema, "payload": silver_user})
            print(f"SUCCESS [USERS]: Sent silver user {silver_user['user_id']}")
        except Exception as e:
            print(f"CRITICAL ERROR [USERS]: {e} -> Sending to DLQ")
            # Send raw bytes or a structured error payload to the DLQ topic
            await users_dlq_topic.send(value=raw_bytes)


@app.agent(products_topic)
async def process_products(stream):
    async for raw_bytes in stream:
        print(f"DEBUG [PRODUCTS]: Received bytes...")
        try:
            product = decode_confluent_avro(raw_bytes)
            if not product: continue
            
            tags_array = product.get("tags") or []
            tags_string = ",".join(tags_array)
            silver_product = {
                "product_id": product.get("product_id"),
                "name": product.get("name"),
                "price": product.get("price"),
                "tags": tags_string,
                "is_digital_download": product.get("is_digital_download")
            }
            await products_silver_topic.send(value={"schema": product_schema, "payload": silver_product})
            print(f"SUCCESS [PRODUCTS]: Sent silver product {silver_product['product_id']}")
        except Exception as e:
            print(f"CRITICAL ERROR [USERS]: {e} -> Sending to DLQ")
            # Send raw bytes or a structured error payload to the DLQ topic
            await products_dlq_topic.send(value=raw_bytes)


@app.agent(orders_topic)
async def process_orders(stream):
    async for raw_bytes in stream:
        print(f"DEBUG [ORDERS]: Received bytes...")
        try:
            order = decode_confluent_avro(raw_bytes)
            if not order: continue
            
            silver_order = {
                "order_id": order.get("order_id"),
                "user_id": order.get("user_id"),
                "total_amount": order.get("total_amount"),
                "discount_applied": order.get("discount_applied"),
                "order_timestamp": order.get("order_timestamp")
            }
            await orders_silver_topic.send(value={"schema": order_schema, "payload": silver_order})
            print(f"SUCCESS [ORDERS]: Sent silver order {silver_order['order_id']}")
        except Exception as e:
            print(f"CRITICAL ERROR [USERS]: {e} -> Sending to DLQ")
            # Send raw bytes or a structured error payload to the DLQ topic
            await orders_dlq_topic.send(value=raw_bytes)


@app.agent(payments_topic)
async def process_payments(stream):
    async for raw_bytes in stream:
        print(f"DEBUG [PAYMENTS]: Received bytes...")
        try:
            payment = decode_confluent_avro(raw_bytes)
            if not payment: continue
            
            await payments_silver_topic.send(value={"schema": payment_schema, "payload": payment})
            print(f"SUCCESS [PAYMENTS]: Sent silver payment {payment['payment_id']}")
        except Exception as e:
            print(f"CRITICAL ERROR [USERS]: {e} -> Sending to DLQ")
            # Send raw bytes or a structured error payload to the DLQ topic
            await payments_dlq_topic.send(value=raw_bytes)

if __name__ == '__main__':
    app.main()