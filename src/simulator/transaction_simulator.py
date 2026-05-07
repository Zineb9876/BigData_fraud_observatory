import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Configuration Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Listes de données réalistes
MERCHANTS = [
    "Amazon", "Carrefour", "Marjane", "Netflix",
    "Uber", "Jumia", "Inwi", "Orange"
]

CITIES = [
    "Casablanca", "Rabat", "Marrakech", "Fes",
    "Tanger", "Agadir", "Paris", "Dubai"
]

def generate_normal_transaction(user_id):
    return {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "amount": round(random.uniform(10, 500), 2),
        "currency": "MAD",
        "merchant": random.choice(MERCHANTS),
        "city": random.choice(CITIES[:5]),
        "timestamp": datetime.now().isoformat(),
        "device": "mobile",
        "is_fraud": False
    }

def generate_fraudulent_transaction(user_id):
    fraud_type = random.choice([
        "high_amount",
        "foreign_location",
        "unusual_hour"
    ])

    transaction = {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "currency": "MAD",
        "merchant": random.choice(MERCHANTS),
        "timestamp": datetime.now().isoformat(),
        "device": "unknown_device",
        "is_fraud": True,
        "fraud_type": fraud_type
    }

    if fraud_type == "high_amount":
        transaction["amount"] = round(random.uniform(5000, 20000), 2)
        transaction["city"] = random.choice(CITIES[:5])

    elif fraud_type == "foreign_location":
        transaction["amount"] = round(random.uniform(100, 2000), 2)
        transaction["city"] = random.choice(["Dubai", "Paris", "London"])

    elif fraud_type == "unusual_hour":
        transaction["amount"] = round(random.uniform(200, 3000), 2)
        transaction["city"] = random.choice(CITIES)

    return transaction

def simulate_transactions(num_transactions=50, fraud_rate=0.2):
    print(f"Démarrage simulation : {num_transactions} transactions")
    print(f"Taux de fraude : {fraud_rate*100}%")
    print("-" * 50)

    users = [f"USER_{i:04d}" for i in range(1, 21)]

    for i in range(num_transactions):
        user_id = random.choice(users)

        if random.random() < fraud_rate:
            transaction = generate_fraudulent_transaction(user_id)
            print(f"FRAUDE  [{i+1:03d}] {transaction['user_id']} "
                  f"| {transaction['amount']} MAD "
                  f"| {transaction['fraud_type']}")
        else:
            transaction = generate_normal_transaction(user_id)
            print(f"NORMAL  [{i+1:03d}] {transaction['user_id']} "
                  f"| {transaction['amount']} MAD "
                  f"| {transaction['city']}")

        producer.send('transactions-stream', value=transaction)
        time.sleep(0.5)

    producer.flush()
    print("-" * 50)
    print("Simulation terminee ! Transactions envoyees dans Kafka.")

if __name__ == "__main__":
    simulate_transactions(num_transactions=50, fraud_rate=0.2)