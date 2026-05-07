import json
import redis
import time
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer
from pymongo import MongoClient

# Connexions
redis_client = redis.Redis(host='localhost', port=6379, db=0)
mongo_client = MongoClient(
    'mongodb://localhost:27017',
    serverSelectionTimeoutMS=5000,
    directConnection=True,
    uuidRepresentation='standard'
)
db = mongo_client['fraud_observatory']
transactions_collection = db['transactions']
alerts_collection = db['alerts']

print("Connexions etablies : Redis + MongoDB")

# Mémoire des transactions récentes
user_transactions = defaultdict(list)

# Règles de détection
def detect_fraud(transaction):
    reasons = []

    if transaction.get('amount', 0) > 3000:
        reasons.append("Montant suspect")

    foreign_cities = ["Dubai", "Paris", "London"]
    if transaction.get('city') in foreign_cities:
        reasons.append("Localisation etrangere")

    if transaction.get('device') == 'unknown_device':
        reasons.append("Appareil inconnu")

    user_id = transaction.get('user_id')
    now = time.time()
    recent = user_transactions[user_id]
    recent = [t for t in recent if now - t < 60]
    recent.append(now)
    user_transactions[user_id] = recent

    if len(recent) > 3:
        reasons.append("Frequence elevee")

    return len(reasons) > 0, reasons

# Sauvegarde Redis
def save_to_redis(transaction, is_fraud, reasons):
    if is_fraud:
        alert = {
            "transaction_id": transaction['transaction_id'],
            "user_id":        transaction['user_id'],
            "amount":         transaction['amount'],
            "city":           transaction.get('city', ''),
            "reasons":        reasons,
            "timestamp":      datetime.now().isoformat()
        }
        redis_client.lpush("fraud_alerts", json.dumps(alert))
        redis_client.ltrim("fraud_alerts", 0, 99)

# Sauvegarde MongoDB
def save_to_mongodb(transaction, is_fraud, reasons):
    doc = {
        "transaction_id": transaction.get('transaction_id'),
        "user_id":        transaction.get('user_id'),
        "amount":         float(transaction.get('amount', 0)),
        "currency":       transaction.get('currency', 'MAD'),
        "merchant":       transaction.get('merchant', ''),
        "city":           transaction.get('city', ''),
        "timestamp":      transaction.get('timestamp'),
        "device":         transaction.get('device', ''),
        "is_fraud":       is_fraud,
        "fraud_reason":   ", ".join(reasons) if reasons else ""
    }
    transactions_collection.insert_one(doc)

    if is_fraud:
        alerts_collection.insert_one({
            "transaction_id": doc["transaction_id"],
            "user_id":        doc["user_id"],
            "amount":         doc["amount"],
            "city":           doc["city"],
            "reasons":        reasons,
            "timestamp":      datetime.now().isoformat()
        })

# Boucle principale
def start_detector():
    print("Moteur de detection demarre...")
    print("En attente de transactions Kafka...")
    print("-" * 60)

    consumer = KafkaConsumer(
        'transactions-stream',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='fraud-detector-group'
    )

    for message in consumer:
        transaction = message.value
        is_fraud, reasons = detect_fraud(transaction)

        save_to_redis(transaction, is_fraud, reasons)
        save_to_mongodb(transaction, is_fraud, reasons)

        timestamp = datetime.now().strftime("%H:%M:%S")
        user   = transaction.get('user_id', '')
        amount = transaction.get('amount', 0)
        city   = transaction.get('city', '')

        if is_fraud:
            print(f"[{timestamp}] ALERTE  | {user} | "
                  f"{amount} MAD | {city} | "
                  f"{' + '.join(reasons)}")
        else:
            print(f"[{timestamp}] NORMAL  | {user} | "
                  f"{amount} MAD | {city}")

if __name__ == "__main__":
    start_detector()