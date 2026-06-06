"""
populate_mongodb.py — Génère des données directement dans MongoDB
avec les vrais labels (sans passer par le détecteur)
"""
import json, random
from datetime import datetime
from faker import Faker
from pymongo import MongoClient

fake = Faker()
client = MongoClient('mongodb://localhost:27017')
db = client['fraud_observatory']
col = db['transactions']

MERCHANTS = ["Amazon", "Carrefour", "Marjane", "Netflix", "Uber", "Jumia"]
NORMAL_CITIES = ["Casablanca", "Rabat", "Marrakech", "Fes", "Tanger", "Agadir"]
FOREIGN_CITIES = ["Dubai", "Paris", "London", "Madrid", "Istanbul"]
DEVICES = ["mobile_ios", "mobile_android", "desktop_chrome", "desktop_firefox"]

def generate(n=1000, fraud_rate=0.10):
    col.delete_many({})
    print(f"Génération de {n} transactions (taux fraude={fraud_rate*100:.0f}%)...")
    users = [f"USER_{i:04d}" for i in range(1, 21)]
    docs = []
    for i in range(n):
        user_id = random.choice(users)
        if random.random() < fraud_rate:
            fraud_type = random.choice(["high_amount", "foreign_location", "unusual_hour"])
            hour = random.randint(1, 4) if fraud_type == "unusual_hour" else random.randint(0, 23)
            city = random.choice(FOREIGN_CITIES) if fraud_type == "foreign_location" else random.choice(NORMAL_CITIES)
            amount = random.uniform(5000, 20000) if fraud_type == "high_amount" else random.uniform(200, 3000)
            docs.append({
                "transaction_id": fake.uuid4(),
                "user_id": user_id,
                "amount": round(amount, 2),
                "currency": "MAD",
                "merchant": random.choice(MERCHANTS),
                "city": city,
                "timestamp": datetime.now().replace(hour=hour).isoformat(),
                "device": "unknown_device",
                "is_fraud": 1,
                "fraud_reason": fraud_type,
                "ml_score": None
            })
        else:
            docs.append({
                "transaction_id": fake.uuid4(),
                "user_id": user_id,
                "amount": round(random.uniform(10, 800), 2),
                "currency": "MAD",
                "merchant": random.choice(MERCHANTS),
                "city": random.choice(NORMAL_CITIES),
                "timestamp": datetime.now().replace(hour=random.randint(0,23), minute=random.randint(0,59)).isoformat(),
                "device": random.choice(DEVICES),
                "is_fraud": 0,
                "fraud_reason": "",
                "ml_score": None
            })
    col.insert_many(docs)
    fraud_n = sum(1 for d in docs if d['is_fraud'])
    print(f"✅ {n} transactions insérées : {fraud_n} fraudes ({fraud_n/n*100:.1f}%), {n-fraud_n} normales")

if __name__ == "__main__":
    generate(n=5000, fraud_rate=0.10)