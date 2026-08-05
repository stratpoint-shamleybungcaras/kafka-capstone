import faust
import struct
import io
import requests
import json
import os
import time
from fastavro import parse_schema, schemaless_reader
from faust.auth import SASLCredentials
from dotenv import load_dotenv
load_dotenv()

# IMPORT CENTRALIZED CONFIGURATION
from config.settings import (
    FAUST_BROKER_URL, 
    SCHEMA_REGISTRY_URL, 
    BRONZE_TOPICS, 
    SILVER_TOPICS,
    DLQ_TOPICS
)

# Faust Configuration
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

# Avro decoder
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

# Topic Trio Creator
def create_topic_trio(entity):
    """Returns a tuple of (bronze, silver, dlq) topics for a given entity."""
    bronze = app.topic(BRONZE_TOPICS[entity], value_serializer='raw')
    silver = app.topic(SILVER_TOPICS[entity])
    dlq    = app.topic(DLQ_TOPICS[entity], value_serializer='json')
    return bronze, silver, dlq

# Unpack topics cleanly
orders_topic, orders_silver_topic, orders_dlq_topic = create_topic_trio('orders')
payments_topic, payments_silver_topic, payments_dlq_topic = create_topic_trio('payments')
products_topic, products_silver_topic, products_dlq_topic = create_topic_trio('products')
users_topic, users_silver_topic, users_dlq_topic = create_topic_trio('users')


# Schema Definitions for Silver Topics (for reference and validation)
user_schema = {
    "type": "struct", "name": "users_silver",  
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
    "type": "struct", "name": "products_silver", 
    "fields": [
        {"type": "string", "optional": True, "field": "product_id"},
        {"type": "string", "optional": True, "field": "name"},
        {"type": "double", "optional": True, "field": "price"},
        {"type": "string", "optional": True, "field": "tags"},
        {"type": "boolean", "optional": True, "field": "is_digital_download"}
    ]
}

order_schema = {
    "type": "struct", "name": "orders_silver", 
    "fields": [
        {"type": "string", "optional": True, "field": "order_id"},
        {"type": "string", "optional": True, "field": "user_id"},
        {"type": "double", "optional": True, "field": "total_amount"},
        {"type": "boolean", "optional": True, "field": "discount_applied"},
        {"type": "int64", "optional": True, "field": "order_timestamp"}
    ]
}

payment_schema = {
    "type": "struct", "name": "payments_silver", 
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

# 4. AGENTS
@app.agent(users_topic)
async def process_users(stream):
    async for raw_bytes in stream:
        try:
            user = decode_confluent_avro(raw_bytes)
            if not user: continue
            
            prefs = user.get("preferences", {})
            
            # DEMO FILTER: Reject users who opted out of emails
            if prefs.get("email_opt_in") is False:
                await users_dlq_topic.send(value={
                    "error_message": "Business Rule: User opted out of marketing emails.",
                    "failed_at": int(time.time() * 1000),
                    "original_payload": user
                })
                continue

            silver_user = {
                "user_id": user.get("user_id"),
                "email": user.get("email"),
                "is_active": user.get("is_active"),
                "pref_email_opt_in": prefs.get("email_opt_in", False),
                "pref_preferred_currency": prefs.get("preferred_currency", "USD"),
                "created_at": user.get("created_at")
            }
            await users_silver_topic.send(value={"schema": user_schema, "payload": silver_user})
        
        except Exception as e:
            await users_dlq_topic.send(value={
                "error_message": f"System Error: {str(e)}",
                "original_payload": "Failed to decode Avro bytes"
            })


@app.agent(products_topic)
async def process_products(stream):
    async for raw_bytes in stream:
        try:
            product = decode_confluent_avro(raw_bytes)
            if not product: continue
            
            # DEMO FILTER: Reject digital downloads
            if product.get("is_digital_download") is True:
                await products_dlq_topic.send(value={
                    "error_message": "Business Rule: Digital downloads are handled by a different pipeline.",
                    "failed_at": int(time.time() * 1000),
                    "original_payload": product
                })
                continue

            tags_array = product.get("tags") or []
            silver_product = {
                "product_id": product.get("product_id"),
                "name": product.get("name"),
                "price": product.get("price"),
                "tags": ",".join(tags_array),
                "is_digital_download": product.get("is_digital_download")
            }
            await products_silver_topic.send(value={"schema": product_schema, "payload": silver_product})
            
        except Exception as e:
            await products_dlq_topic.send(value={
                "error_message": f"System Error: {str(e)}",
                "original_payload": "Failed to decode Avro bytes"
            })


@app.agent(orders_topic)
async def process_orders(stream):
    async for raw_bytes in stream:
        try:
            order = decode_confluent_avro(raw_bytes)
            if not order: continue
            
            # DEMO FILTER: Reject micro-orders (less than $15)
            if order.get("total_amount", 0) < 15.00:
                await orders_dlq_topic.send(value={
                    "error_message": "Business Rule: Order total below minimum threshold ($15.00).",
                    "failed_at": int(time.time() * 1000),
                    "original_payload": order
                })
                continue

            silver_order = {
                "order_id": order.get("order_id"),
                "user_id": order.get("user_id"),
                "total_amount": order.get("total_amount"),
                "discount_applied": order.get("discount_applied"),
                "order_timestamp": order.get("order_timestamp")
            }
            await orders_silver_topic.send(value={"schema": order_schema, "payload": silver_order})
            
        except Exception as e:
            await orders_dlq_topic.send(value={
                "error_message": f"System Error: {str(e)}",
                "original_payload": "Failed to decode Avro bytes"
            })


@app.agent(payments_topic)
async def process_payments(stream):
    async for raw_bytes in stream:
        try:
            payment = decode_confluent_avro(raw_bytes)
            if not payment: continue
            
            # DEMO FILTER: Reject failed payments
            if payment.get("status") == "FAILED":
                await payments_dlq_topic.send(value={
                    "error_message": "Business Rule: Payment status is FAILED.",
                    "failed_at": int(time.time() * 1000),
                    "original_payload": payment
                })
                continue 
            
            await payments_silver_topic.send(value={"schema": payment_schema, "payload": payment})
            
        except Exception as e:
            await payments_dlq_topic.send(value={
                "error_message": f"System Error: {str(e)}",
                "original_payload": "Failed to decode Avro bytes"
            })
            
if __name__ == '__main__':
    app.main()