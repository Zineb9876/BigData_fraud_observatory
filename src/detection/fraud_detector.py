import warnings
warnings.filterwarnings("ignore")
import os
os.environ["PYTHONWARNINGS"] = "ignore"
import json
import redis
import time
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer
from pymongo import MongoClient

# â”€â”€ Connexions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

# â”€â”€ Chargement optionnel du modÃ¨le ML â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
import warnings
warnings.filterwarnings("ignore")
ml_available = False
print("[ML] Mode rÃ¨gles seules â€” ML dÃ©sactivÃ©")

# â”€â”€ MÃ©moire des transactions rÃ©centes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
user_transactions = defaultdict(list)

# Villes Ã©trangÃ¨res (correction : Paris et Dubai retirÃ©s de la liste normale)
FOREIGN_CITIES = {"Dubai", "Paris", "London", "Madrid", "Istanbul"}


# â”€â”€ RÃ¨gles de dÃ©tection â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def detect_fraud_rules(transaction):
    signals = []

    # Signal 1 : montant trÃ¨s Ã©levÃ©
    if transaction.get('amount', 0) > 15000:
        signals.append("Montant suspect")

    # Signal 2 : ville Ã©trangÃ¨re
    if transaction.get('city') in FOREIGN_CITIES:
        signals.append("Localisation etrangere")

    # Signal 3 : appareil inconnu (seul ne suffit pas)
    if transaction.get('device') == 'unknown_device':
        signals.append("Appareil inconnu")

    # Signal 4 : heure suspecte (1h-4h)
    try:
        hour = datetime.fromisoformat(
            transaction.get('timestamp', datetime.now().isoformat())
        ).hour
        if 1 <= hour <= 4:
            signals.append("Heure suspecte")
    except Exception:
        pass

    # Signal 5 : frÃ©quence Ã©levÃ©e (>5 en 60s seulement)
    user_id = transaction.get('user_id')
    now = time.time()
    recent = [t for t in user_transactions[user_id] if now - t < 60]
    recent.append(now)
    user_transactions[user_id] = recent
    if len(recent) > 5:
        signals.append("Frequence elevee")

    # Fraude seulement si 2+ signaux, OU ville Ã©trangÃ¨re seule, OU montant seul
    strong = {"Localisation etrangere", "Montant suspect (>5000 MAD)"}
    has_strong = any(s in strong for s in signals)
    
    if has_strong or len(signals) >= 2:
        return signals
    return []


# â”€â”€ DÃ©tection hybride (rÃ¨gles + ML) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def detect_fraud(transaction):
    reasons = detect_fraud_rules(transaction)
    ml_score = None
    ml_fraud = False

    if ml_available:
        try:
            result = score_transaction(transaction)
            ml_score = result['fraud_score']
            ml_fraud = result['is_fraud_ml']
            confidence = result['confidence']

            if ml_fraud and confidence in ('HIGH', 'MEDIUM'):
                reasons.append(f"ML score={ml_score:.2f} ({confidence})")
        except Exception as e:
            pass  # Ne pas bloquer si ML Ã©choue

    # Fraude si rÃ¨gles OU ML dÃ©tectent quelque chose
    is_fraud = len(reasons) > 0

    return is_fraud, reasons, ml_score


# â”€â”€ Sauvegarde Redis â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def save_to_redis(transaction, is_fraud, reasons):
    if is_fraud:
        alert = {
            "transaction_id": transaction['transaction_id'],
            "user_id":        transaction['user_id'],
            "amount":         transaction.get('amount', 0),
            "city":           transaction.get('city', ''),
            "fraud_reasons": reasons,
            "timestamp":      datetime.now().isoformat()
        }
        redis_client.lpush("fraud_alerts", json.dumps(alert))
        redis_client.ltrim("fraud_alerts", 0, 499)


# â”€â”€ Sauvegarde MongoDB â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def save_to_mongodb(transaction, is_fraud, reasons, ml_score=None):
    doc = {
        "transaction_id": transaction.get('transaction_id'),
        "user_id":        transaction.get('user_id'),
        "amount":         float(transaction.get('amount', 0)),
        "currency":       transaction.get('currency', 'MAD'),
        "merchant":       transaction.get('merchant', ''),
        "city":           transaction.get('city', ''),
        "timestamp":      transaction.get('timestamp'),
        "device":         transaction.get('device', ''),
        "is_fraud":       1 if is_fraud else 0,
        "fraud_reason": reasons[0] if reasons else "",
        "ml_score":       ml_score
    }
    transactions_collection.insert_one(doc)

    if is_fraud:
        alerts_collection.insert_one({
            "transaction_id": doc["transaction_id"],
            "user_id":        doc["user_id"],
            "amount":         doc["amount"],
            "city":           doc["city"],
            "fraud_reasons": reasons,
            "ml_score":       ml_score,
            "timestamp":      datetime.now().isoformat()
        })


# â”€â”€ Boucle principale â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def start_detector():
    print("Moteur de detection demarre...")
    print(f"Mode : {'Hybride (RÃ¨gles + ML)' if ml_available else 'RÃ¨gles seules'}")
    print("En attente de transactions Kafka...")
    print("-" * 70)

    consumer = KafkaConsumer(
        'transactions-stream',
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='fraud-detector-v2'
    )

    for message in consumer:
        transaction = message.value
        is_fraud, reasons, ml_score = detect_fraud(transaction)

        save_to_redis(transaction, is_fraud, reasons)
        save_to_mongodb(transaction, is_fraud, reasons, ml_score)

        ts     = datetime.now().strftime("%H:%M:%S")
        user   = transaction.get('user_id', '')
        amount = transaction.get('amount', 0)
        city   = transaction.get('city', '')
        ml_str = f" | ML={ml_score:.2f}" if ml_score is not None else ""

        if is_fraud:
            print(f"[{ts}] ðŸš¨ ALERTE | {user} | {amount} MAD | {city}"
                  f"{ml_str} | {' + '.join(reasons)}")
        else:
            print(f"[{ts}] âœ… NORMAL | {user} | {amount} MAD | {city}{ml_str}")


if __name__ == "__main__":
    start_detector()


