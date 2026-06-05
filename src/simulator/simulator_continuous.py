"""
Simulateur realiste - Real-Time Fraud Observatory
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

FRAUD_RATE    = 0.02
TX_DELAY_MIN  = 2.0
TX_DELAY_MAX  = 4.0

def make_transaction():
    user_id   = random.choice(USERS)
    is_fraud  = random.random() < FRAUD_RATE
    ftype     = None

    if is_fraud:
        ftype  = random.choice(["high_amount","foreign_city","rapid_succession","unusual_device"])
        amount = round(random.uniform(5000, 50000), 2)
        city   = random.choice(FOREIGN_CITIES)
    else:
        amount = round(random.uniform(50, 3000), 2)
        city   = random.choice(NORMAL_CITIES)

    tx = {
        "transaction_id": f"TX_{int(time.time()*1000)}_{random.randint(1000,9999)}",
        "user_id":        user_id,
        "amount":         amount,
        "merchant":       random.choice(MERCHANTS),
        "city":           city,
        "device":         random.choice(DEVICES),
        "timestamp":      datetime.now().isoformat(),
        "is_fraud":       is_fraud,
    }
    return tx, is_fraud, ftype

def make_producer():
    for attempt in range(10):
        try:
            p = KafkaProducer(
                bootstrap_servers=["localhost:29092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
                api_version=(2, 5, 0),
            )
            print("Kafka connecte")
            return p
        except Exception as e:
            print(f"Tentative {attempt+1}/10 - {e}")
            time.sleep(5)
    raise RuntimeError("Kafka inaccessible apres 10 tentatives")

if __name__ == "__main__":
    print("=" * 60)
    print("  FRAUD OBSERVATORY - Simulateur realiste")
    print(f"  Taux fraude: {FRAUD_RATE*100:.0f}% | Delai: {TX_DELAY_MIN}-{TX_DELAY_MAX}s/tx")
    print("=" * 60)

    producer  = make_producer()
    tx_count  = 0
    fraud_count = 0

    while True:
        tx, is_fraud, ftype = make_transaction()
        producer.send("transactions-stream", value=tx)
        producer.flush()
        tx_count += 1
        if is_fraud:
            fraud_count += 1
            print(f"[{datetime.now().strftime('%H:%M:%S')}] FRAUDE #{tx_count} | {tx['user_id']} | {tx['amount']:>10.2f} MAD | {tx['city']} | {ftype}")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] NORMAL #{tx_count} | {tx['user_id']} | {tx['amount']:>10.2f} MAD | {tx['city']}")
        if tx_count % 50 == 0:
            taux = round(fraud_count/tx_count*100, 1)
            print(f"\n  Stats: {tx_count} tx | {fraud_count} fraudes | taux={taux}%\n")
        delay = random.uniform(TX_DELAY_MIN, TX_DELAY_MAX)
        time.sleep(delay)
