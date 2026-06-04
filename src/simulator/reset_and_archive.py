"""
Reset quotidien + Archivage — Real-Time Fraud Observatory
- S'exécute à 6h du matin chaque jour
- Archive les données dans history_YYYY-MM-DD
- Repart de zéro pour les stats du jour
- Lance avec: python reset_and_archive.py
"""
from pymongo import MongoClient
from datetime import datetime, timedelta
import schedule, time, json, os

MONGO_URL = os.environ.get("MONGODB_URL", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URL)
db = client["fraud_observatory"]

def archive_and_reset():
    now = datetime.now()
    date_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"\n[{now.strftime('%H:%M:%S')}] 🔄 Reset quotidien — archivage de {date_str}")

    since_yesterday = (now - timedelta(hours=24)).isoformat()

    # ── 1. Collecter les stats du jour ────────────────────
    tx_total   = db.transactions.count_documents({"timestamp": {"$gte": since_yesterday}})
    tx_frauds  = db.transactions.count_documents({"is_fraud": 1, "timestamp": {"$gte": since_yesterday}})
    tx_normals = tx_total - tx_frauds
    taux = round(tx_frauds / max(1, tx_total) * 100, 2)

    amt_pipe = [
        {"$match": {"is_fraud": 1, "timestamp": {"$gte": since_yesterday}}},
        {"$group": {"_id": None,
                    "total": {"$sum": "$amount"},
                    "avg":   {"$avg": "$amount"},
                    "max":   {"$max": "$amount"}}}
    ]
    amt = list(db.transactions.aggregate(amt_pipe))
    a = amt[0] if amt else {}

    # Top fraudeurs
    top_pipe = [
        {"$match": {"is_fraud": 1, "timestamp": {"$gte": since_yesterday}}},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}, "total": {"$sum": "$amount"}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    top_users = list(db.transactions.aggregate(top_pipe))

    # Par ville
    city_pipe = [
        {"$match": {"timestamp": {"$gte": since_yesterday}}},
        {"$group": {"_id": "$city",
                    "total":  {"$sum": 1},
                    "frauds": {"$sum": {"$cond": [{"$eq": ["$is_fraud", 1]}, 1, 0]}}}},
        {"$sort": {"frauds": -1}}
    ]
    by_city = list(db.transactions.aggregate(city_pipe))

    # Par type de fraude
    type_pipe = [
        {"$match": {"is_fraud": 1, "timestamp": {"$gte": since_yesterday}}},
        {"$group": {"_id": "$fraud_reason", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    by_type = list(db.transactions.aggregate(type_pipe))

    # ── 2. Archiver dans history ───────────────────────────
    archive = {
        "date":              date_str,
        "archived_at":       now.isoformat(),
        "summary": {
            "total_transactions": tx_total,
            "total_frauds":       tx_frauds,
            "total_normals":      tx_normals,
            "fraud_rate_pct":     taux,
            "fraud_total_amount": round(a.get("total", 0), 2),
            "fraud_avg_amount":   round(a.get("avg",   0), 2),
            "fraud_max_amount":   round(a.get("max",   0), 2),
        },
        "top_fraudsters": [
            {"user_id": r["_id"], "fraud_count": r["count"],
             "total_amount": round(r["total"], 2)}
            for r in top_users
        ],
        "by_city": [
            {"city": r["_id"], "total": r["total"], "frauds": r["frauds"]}
            for r in by_city if r["_id"]
        ],
        "by_fraud_type": [
            {"type": r["_id"] or "Autre", "count": r["count"]}
            for r in by_type
        ],
    }
    db.history.insert_one(archive)
    print(f"  ✅ Archivé dans 'history' — {tx_total} tx, {tx_frauds} fraudes ({taux}%)")

    # ── 3. Supprimer les données de la veille ─────────────
    # Garder seulement les 24 dernières heures
    cutoff = (now - timedelta(hours=24)).isoformat()
    del_tx = db.transactions.delete_many({"timestamp": {"$lt": cutoff}})
    del_al = db.alerts.delete_many({"timestamp": {"$lt": cutoff}})
    print(f"  🗑️  Supprimé: {del_tx.deleted_count} transactions, {del_al.deleted_count} alertes")
    print(f"  ✅ Reset terminé — nouveau cycle 24h démarré\n")

def run_scheduler():
    print("=" * 60)
    print("  🛡️  FRAUD OBSERVATORY — Reset & Archive quotidien")
    print(f"  Reset programmé à 06:00 chaque jour")
    print("=" * 60)

    # Planifier à 6h du matin
    schedule.every().day.at("06:00").do(archive_and_reset)

    # Aussi disponible manuellement
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        print("  ▶ Exécution manuelle immédiate...")
        archive_and_reset()
        return

    print(f"  ⏳ En attente... prochain reset à 06:00")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    run_scheduler()
