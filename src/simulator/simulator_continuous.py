"""
Simulateur réaliste — Real-Time Fraud Observatory
- 1 transaction toutes les 2-3 secondes (réaliste)
- Taux fraude 2-3% (réaliste)
- Fraudes espacées naturellement
"""
import json, random, time, sys
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

MERCHANTS      = ["Marjane","Carrefour","Acima","BIM","Label'Vie","Inwi","Maroc Telecom","Orange","ONCF","RAM"]
NORMAL_CITIES  = ["Casablanca","Rabat","Marrakech","Fes","Tanger","Agadir","Oujda","Meknes","Kenitra","Tetouan"]
FOREIGN_CITIES = ["Dubai","Paris","London","Madrid","Istanbul","New York","Berlin","Rome"]
DEVICES        = ["mobile_ios","mobile_android","desktop_chrome","desktop_firefox","tablet_safari"]
USERS          = [f"USER_{i:04d}" for i in range(1, 31)]

# Taux de fraude réaliste: 2%
FRAUD_RATE = 0.02
# Délai entre transactions: 2-4 secondes (réaliste)
TX_DELAY_MIN = 2.0
TX_DELAY_MAX = 4.0

def make_producer():
    for attempt in range(10):
        try:
            p = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
                api_version=(2, 5, 0),
            )
            print("✅ Kafka connecté")
            return p
        except Exception as e:
            print(f"⏳ Tentative {attempt+1}/10 — {e}")
            time.sleep(5)
    print("❌ Kafka indisponible")
    sys.exit(1)

def make_transaction():
    uid = random.choice(USERS)
    is_fraud = random.random() < FRAUD_RATE

    if is_fraud:
        ftype = random.choice(["high_amount", "foreign_location", "unusual_hour"])
        tx = {
            "transaction_id": fake.uuid4(),
            "user_id":        uid,
            "currency":       "MAD",
            "merchant":       random.choice(MERCHANTS),
            "device":         "unknown_device",
            "is_fraud":       1,
            "fraud_reason":   ftype,
            "timestamp":      datetime.now().isoformat(),
        }
        if ftype == "high_amount":
            tx["amount"] = round(random.uniform(5000, 22000), 2)
            tx["city"]   = random.choice(NORMAL_CITIES)
        elif ftype == "foreign_location":
            tx["amount"] = round(random.uniform(500, 8000), 2)
            tx["city"]   = random.choice(FOREIGN_CITIES)
        else:
            tx["amount"] = round(random.uniform(300, 5000), 2)
            tx["city"]   = random.choice(NORMAL_CITIES)
        return tx, True, ftype
    else:
        tx = {
            "transaction_id": fake.uuid4(),
            "user_id":        uid,
            "amount":         round(random.uniform(10, 900), 2),
            "currency":       "MAD",
            "merchant":       random.choice(MERCHANTS),
            "city":           random.choice(NORMAL_CITIES),
            "timestamp":      datetime.now().isoformat(),
            "device":         random.choice(DEVICES),
            "is_fraud":       0,
            "fraud_reason":   "",
        }
        return tx, False, None

if __name__ == "__main__":
    print("=" * 60)
    print("  🛡️  FRAUD OBSERVATORY — Simulateur réaliste")
    print(f"  Taux fraude: {FRAUD_RATE*100:.0f}% | Délai: {TX_DELAY_MIN}-{TX_DELAY_MAX}s/tx")
    print("=" * 60)

    producer = make_producer()
    tx_count = 0
    fraud_count = 0

    while True:
        tx, is_fraud, ftype = make_transaction()
        producer.send("transactions-stream", value=tx)
        producer.flush()
        tx_count += 1

        if is_fraud:
            fraud_count += 1
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔴 FRAUDE #{tx_count} | {tx['user_id']} | {tx['amount']:>10.2f} MAD | {tx['city']:<15} | {ftype}")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 NORMAL #{tx_count} | {tx['user_id']} | {tx['amount']:>10.2f} MAD | {tx['city']}")

        if tx_count % 50 == 0:
            taux = round(fraud_count/tx_count*100, 1)
            print(f"\n  📊 Stats: {tx_count} tx | {fraud_count} fraudes | taux={taux}%\n")

        # Délai réaliste entre transactions
        delay = random.uniform(TX_DELAY_MIN, TX_DELAY_MAX)
        time.sleep(delay)
