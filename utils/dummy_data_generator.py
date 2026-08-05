import random
import uuid
import time
from typing import Dict, Any, List
from faker import Faker

fake = Faker()

class EcommerceDataGenerator:
    def __init__(self, seed: int = None):
        if seed:
            random.seed(seed)
            Faker.seed(seed)
        self.user_pool: List[str] = []
        self.product_pool: List[str] = []

    def get_user_id(self) -> str:
        if self.user_pool and random.random() > 0.2:
            return random.choice(self.user_pool)
        new_id = f"user_{uuid.uuid4().hex[:8]}"
        self.user_pool.append(new_id)
        return new_id

    def get_product_id(self) -> str:
        if self.product_pool and random.random() > 0.1:
            return random.choice(self.product_pool)
        new_id = f"prod_{uuid.uuid4().hex[:8]}"
        self.product_pool.append(new_id)
        return new_id

    def generate_user(self) -> Dict[str, Any]:
        return {
            "user_id": self.get_user_id(),
            "email": fake.email(),
            "is_active": True,
            "preferences": {
                "email_opt_in": fake.boolean(chance_of_getting_true=70),
                "preferred_currency": random.choice(["USD", "EUR", "GBP"]),
            },
            "created_at": int(time.time() * 1000)
        }

    def generate_product(self) -> Dict[str, Any]:
        return {
            "product_id": self.get_product_id(),
            "name": fake.company() + " " + fake.word().capitalize(),
            "price": round(random.uniform(10.0, 999.99), 2),
            "tags": [fake.word(), fake.word(), "sale"],
            "is_digital_download": fake.boolean(chance_of_getting_true=20)
        }

    def generate_order(self) -> Dict[str, Any]:
        num_items = random.randint(1, 3)
        items = []
        total = 0.0

        for _ in range(num_items):
            price = round(random.uniform(2.0, 100.0), 2)
            qty = random.randint(1, 4)
            items.append({
                "product_id": self.get_product_id(),
                "quantity": qty,
                "unit_price": price
            })
            total += (price * qty)

        return {
            "order_id": f"ord_{uuid.uuid4().hex[:10]}",
            "user_id": self.get_user_id(),
            "items": items,
            "total_amount": round(total, 2),
            "discount_applied": fake.boolean(chance_of_getting_true=30),
            "order_timestamp": int(time.time() * 1000) 
        }

    def generate_payment(self, order_id: str, amount: float) -> Dict[str, Any]:
        is_success = random.random() > 0.1 # 90% SUCCESS, 10% FAILED
        
        return {
            "payment_id": f"pay_{uuid.uuid4().hex[:10]}",
            "order_id": order_id,
            "amount": amount,
            "status": "SUCCESS" if is_success else "FAILED", 
            "risk_score": round(random.uniform(0.0, 1.0), 2),
            "card_network": random.choice(["VISA", "MASTERCARD", "AMEX", None]), 
            "timestamp": int(time.time() * 1000)
        }