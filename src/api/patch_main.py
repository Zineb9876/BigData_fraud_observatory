"""Patch main.py inside Docker to read from alerts + spark_alerts collections."""
import re

path = '/app/main.py'
with open(path) as f:
    src = f.read()

print("Before - fraud_total line:")
for line in src.splitlines():
    if 'fraud_total' in line and 'count_documents' in line:
        print(" ", line)

# Fix 1: get_stats — read from alerts collection
src = src.replace(
    'fraud_total = transactions_col.count_documents({"is_fraud": 1})',
    '''fraud_from_alerts = db["alerts"].count_documents({})
    fraud_from_spark  = db["spark_alerts"].count_documents({})
    fraud_total = max(transactions_col.count_documents({"is_fraud": 1}), fraud_from_alerts)'''
)

# Fix 2: amounts — read from alerts collection
src = src.replace(
    '''    pipeline_amount = [
        {"$group": {
            "_id":          {"$cond": [{"$gt": ["$is_fraud", 0]}, True, False]},
            "total_amount": {"$sum": "$amount"},
            "avg_amount":   {"$avg": "$amount"},
            "max_amount":   {"$max": "$amount"},
        }}
    ]
    amounts = {}
    for r in transactions_col.aggregate(pipeline_amount):
        amounts[r["_id"]] = r''',
    '''    # Use alerts collection for amounts (more recent data)
    amt_pipe = [{"$group": {"_id": None, "total_amount": {"$sum": "$amount"},
                             "avg_amount": {"$avg": "$amount"}, "max_amount": {"$max": "$amount"}}}]
    amt_res = list(db["alerts"].aggregate(amt_pipe))
    amt = amt_res[0] if amt_res else {}
    amounts = {True: {"total_amount": amt.get("total_amount", 0),
                      "avg_amount":   amt.get("avg_amount", 0),
                      "max_amount":   amt.get("max_amount", 0)}}'''
)

# Fix 3: /alerts/recent — read from alerts collection first
old_recent_fallback = '''    # Fallback MongoDB transactions fraudes
    docs = list(transactions_col.find(
        {"is_fraud": {"$ne": 0}}, {"_id": 0}
    ).sort("timestamp", -1).limit(limit))'''

new_recent_fallback = '''    # Try alerts collection first
    docs = list(db["alerts"].find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
    if docs:
        for d in docs:
            r = d.get("reasons", d.get("fraud_reasons", []))
            if isinstance(r, str): r = [r]
            d["fraud_reasons"] = r
        return {"source": "alerts_collection", "count": len(docs), "data": docs}

    # Try spark_alerts
    spark_docs = list(db["spark_alerts"].find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
    if spark_docs:
        docs = [{"user_id": d.get("user_id","?"), "amount": d.get("montant_total",0),
                 "city": (d.get("villes") or ["—"])[0], "fraud_reasons": d.get("reasons",[]),
                 "timestamp": d.get("timestamp","")} for d in spark_docs]
        return {"source": "spark_alerts", "count": len(docs), "data": docs}

    # Fallback MongoDB transactions fraudes
    docs = list(transactions_col.find(
        {"is_fraud": {"$ne": 0}}, {"_id": 0}
    ).sort("timestamp", -1).limit(limit))'''

src = src.replace(old_recent_fallback, new_recent_fallback)

# Fix 4: top-users — read from alerts collection
old_top_match = '{"$match": {"is_fraud": {"$ne": 0}}},'
new_top_match = '{"$match": {}},'  # alerts collection has only frauds
src = src.replace(
    '''    pipeline = [
        {"$match": {"is_fraud": {"$ne": 0}}},
        {"$group": {
            "_id":          "$user_id",
            "fraud_count":  {"$sum": 1},
            "total_amount": {"$sum": "$amount"},
            "last_fraud":   {"$max": "$timestamp"},
        }},
        {"$sort": {"fraud_count": -1}},
        {"$limit": limit}
    ]
    results = list(transactions_col.aggregate(pipeline))''',
    '''    # Use alerts collection for top users (more recent)
    pipeline = [
        {"$group": {"_id": "$user_id", "fraud_count": {"$sum": 1},
                    "total_amount": {"$sum": "$amount"}, "last_fraud": {"$max": "$timestamp"}}},
        {"$sort": {"fraud_count": -1}},
        {"$limit": limit}
    ]
    results = list(db["alerts"].aggregate(pipeline))
    if not results:
        pipeline2 = [
            {"$match": {"is_fraud": 1}},
            {"$group": {"_id": "$user_id", "fraud_count": {"$sum": 1},
                        "total_amount": {"$sum": "$amount"}, "last_fraud": {"$max": "$timestamp"}}},
            {"$sort": {"fraud_count": -1}}, {"$limit": limit}
        ]
        results = list(transactions_col.aggregate(pipeline2))'''
)

with open(path, 'w') as f:
    f.write(src)

print("After - fraud_total lines:")
for line in src.splitlines():
    if 'fraud_total' in line or 'fraud_from_alerts' in line:
        print(" ", line.strip())

print("alerts_collection in src:", 'alerts_collection' in src)
print("spark_alerts in src:", 'spark_alerts' in src)
print("PATCH COMPLETE")
