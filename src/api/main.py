"""
Real-Time Fraud Observatory — API REST v4
LOGIQUE FINALE CORRECTE:
  - total_transactions = col_tx (normales + fraudes) sur 24h
  - total_frauds       = col_tx is_fraud=1 sur 24h  (source unique fiable)
  - total_normals      = total - frauds
  - fraud_rate         = frauds/total*100
  - alerts collection  = seulement pour le feed temps réel
  - auto_offset_reset  = latest dans fraud_detector (pas de duplication)
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from datetime import datetime, timedelta
import pymongo, os, redis, json, uuid as _uuid

app = FastAPI(title="Real-Time Fraud Observatory API", version="4.0.0", docs_url="/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MONGO_URL = os.environ.get("MONGODB_URL", "mongodb://mongodb:27017/")
mongo_client = pymongo.MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db        = mongo_client["fraud_observatory"]
col_tx    = db["transactions"]
col_alert = db["alerts"]
col_spark = db["spark_alerts"]

try:
    REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
    host = REDIS_URL.replace("redis://","").split(":")[0]
    redis_client = redis.Redis(host=host, port=6379, db=0, decode_responses=True)
    redis_client.ping()
except:
    redis_client = None

FOREIGN = {"Dubai","Paris","London","Madrid","Istanbul","New York","Berlin","Rome"}

def since_24h():
    return (datetime.now() - timedelta(hours=24)).isoformat()

def enrich(doc):
    r = doc.get("fraud_reasons") or doc.get("reasons") or []
    if isinstance(r, str):
        M = {"high_amount":"Montant suspect",
             "foreign_location":"Localisation étrangère",
             "unusual_hour":"Heure suspecte"}
        r = [M.get(r, r)] if r else []
    if not isinstance(r, list): r = []
    fr = doc.get("fraud_reason","")
    M2 = {"high_amount":"Montant suspect",
          "foreign_location":"Localisation étrangère",
          "unusual_hour":"Heure suspecte"}
    if fr and M2.get(fr) and M2[fr] not in r:
        r.append(M2[fr])
    city = doc.get("city","")
    amt  = doc.get("amount", 0)
    if city in FOREIGN and "Localisation étrangère" not in r:
        r.append("Localisation étrangère")
    if doc.get("device") == "unknown_device" and "Appareil inconnu" not in r:
        r.append("Appareil inconnu")
    if amt > 5000 and "Montant suspect" not in r:
        r.append("Montant suspect")
    doc["fraud_reasons"] = r
    return doc

def normalize(doc):
    doc.pop("_id", None)
    enrich(doc)
    if "montant_total" in doc and "amount" not in doc:
        doc["amount"] = doc["montant_total"]
    if "villes" in doc and "city" not in doc:
        doc["city"] = (doc["villes"] or ["—"])[0]
    return doc

@app.get("/")
def root():
    return {"name":"Fraud Observatory API","version":"4.0.0","status":"running"}

@app.get("/health")
def health():
    svc = {"api":"ok","mongodb":"unknown","redis":"unknown"}
    try: mongo_client.admin.command("ping"); svc["mongodb"]="ok"
    except Exception as e: svc["mongodb"]=str(e)[:80]
    try:
        if redis_client: redis_client.ping(); svc["redis"]="ok"
        else: svc["redis"]="unavailable"
    except Exception as e: svc["redis"]=str(e)[:80]
    return {"status":"healthy" if svc["mongodb"]=="ok" else "degraded","services":svc}

@app.get("/stats")
def get_stats():
    """
    SOURCE UNIQUE: col_tx (transactions)
    total = toutes tx 24h
    frauds = tx avec is_fraud=1 24h
    normals = tx avec is_fraud=0 24h
    rate = frauds/total*100
    """
    since = since_24h()

    # Source unique et fiable: col_tx
    total_24h   = col_tx.count_documents({"timestamp": {"$gte": since}})
    frauds_24h  = col_tx.count_documents({"is_fraud": 1, "timestamp": {"$gte": since}})
    normals_24h = col_tx.count_documents({"is_fraud": 0, "timestamp": {"$gte": since}})
    taux        = round(frauds_24h / max(1, total_24h) * 100, 1)

    # Montants fraudes 24h
    amt_pipe = [
        {"$match": {"is_fraud": 1, "timestamp": {"$gte": since}}},
        {"$group": {"_id": None,
                    "total": {"$sum": "$amount"},
                    "avg":   {"$avg": "$amount"},
                    "mx":    {"$max": "$amount"}}}
    ]
    amt = list(col_tx.aggregate(amt_pipe))
    a = amt[0] if amt else {}

    # Spark alerts 24h
    n_spark = col_spark.count_documents({"timestamp": {"$gte": since}})

    # Top users 24h depuis col_tx
    tu_pipe = [
        {"$match":  {"is_fraud": 1, "timestamp": {"$gte": since}}},
        {"$group":  {"_id": "$user_id",
                     "fraud_count":  {"$sum": 1},
                     "total_amount": {"$sum": "$amount"}}},
        {"$sort":   {"fraud_count": -1}},
        {"$limit":  5}
    ]
    top_users = list(col_tx.aggregate(tu_pipe))

    return {
        "overview": {
            "total_transactions": total_24h,
            "total_frauds":       frauds_24h,
            "total_normals":      normals_24h,
            "fraud_rate_pct":     taux,
            "spark_alerts":       n_spark,
            "period":             "24h",
            "since":              since,
        },
        "amounts": {
            "fraud_total_amount": round(a.get("total", 0), 2),
            "fraud_avg_amount":   round(a.get("avg",   0), 2),
            "fraud_max_amount":   round(a.get("mx",    0), 2),
        },
        "top_users": [
            {"user_id":      r["_id"],
             "fraud_count":  r["fraud_count"],
             "total_amount": round(r["total_amount"], 2)}
            for r in top_users
        ],
        "generated_at": datetime.now().isoformat(),
    }

@app.get("/alerts/recent")
def get_recent_alerts(limit: int = Query(50, ge=1, le=200)):
    since = since_24h()
    # 1. Redis
    if redis_client:
        try:
            raw  = redis_client.lrange("fraud_alerts", 0, limit-1)
            data = [json.loads(r) for r in raw]
            if data:
                return {"source":"redis","count":len(data),
                        "data":[normalize(d) for d in data]}
        except:
            pass
    # 2. Alerts 24h
    docs = list(col_alert.find({"timestamp":{"$gte":since}},{"_id":0})
                          .sort("timestamp",-1).limit(limit))
    if docs:
        return {"source":"alerts_24h","count":len(docs),
                "data":[normalize(d) for d in docs]}
    # 3. Transactions fraudes 24h
    docs = list(col_tx.find({"is_fraud":1,"timestamp":{"$gte":since}},{"_id":0})
                       .sort("timestamp",-1).limit(limit))
    if docs:
        return {"source":"transactions_24h","count":len(docs),
                "data":[normalize(d) for d in docs]}
    # 4. Toutes transactions fraudes
    docs = list(col_tx.find({"is_fraud":1},{"_id":0})
                       .sort("timestamp",-1).limit(limit))
    return {"source":"transactions_all","count":len(docs),
            "data":[normalize(d) for d in docs]}

@app.get("/alerts")
def get_alerts(limit:   int = Query(50,ge=1,le=500),
               skip:    int = Query(0,ge=0),
               user_id: Optional[str] = None):
    since = since_24h()
    q = {"is_fraud": 1, "timestamp": {"$gte": since}}
    if user_id: q["user_id"] = user_id
    total = col_tx.count_documents(q)
    docs  = [normalize(d) for d in
             col_tx.find(q,{"_id":0}).sort("timestamp",-1).skip(skip).limit(limit)]
    return {"total":total,"data":docs}

@app.get("/alerts/summary")
def get_alerts_summary(days: int = Query(1,ge=1,le=90)):
    since = (datetime.now()-timedelta(days=days)).isoformat()
    M = {"high_amount":"Montant suspect",
         "foreign_location":"Localisation étrangère",
         "unusual_hour":"Heure suspecte"}
    pipe = [
        {"$match":  {"is_fraud":1,"timestamp":{"$gte":since}}},
        {"$group":  {"_id":"$fraud_reason",
                     "count":        {"$sum":1},
                     "total_amount": {"$sum":"$amount"},
                     "avg_amount":   {"$avg":"$amount"}}},
        {"$sort":   {"count":-1}}
    ]
    results = list(col_tx.aggregate(pipe))
    return {"period_days":days,"by_type":[
        {"fraud_type":   M.get(r["_id"], r["_id"] or "Autre"),
         "count":        r["count"],
         "total_amount": round(r["total_amount"],2),
         "avg_amount":   round(r["avg_amount"],2)}
        for r in results
    ]}

@app.get("/transactions")
def get_transactions(limit:      int = Query(50,ge=1,le=500),
                     skip:       int = Query(0,ge=0),
                     is_fraud:   Optional[bool]  = None,
                     user_id:    Optional[str]   = None,
                     city:       Optional[str]   = None,
                     min_amount: Optional[float] = None,
                     max_amount: Optional[float] = None):
    q = {}
    if is_fraud is not None: q["is_fraud"] = 1 if is_fraud else 0
    if user_id:  q["user_id"] = user_id
    if city:     q["city"]    = city
    if min_amount or max_amount:
        q["amount"] = {}
        if min_amount: q["amount"]["$gte"] = min_amount
        if max_amount: q["amount"]["$lte"] = max_amount
    total = col_tx.count_documents(q)
    docs  = list(col_tx.find(q,{"_id":0}).sort("timestamp",-1).skip(skip).limit(limit))
    return {"total":total,"skip":skip,"limit":limit,"data":docs}

@app.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str):
    doc = col_tx.find_one({"transaction_id":transaction_id},{"_id":0})
    if not doc: raise HTTPException(404,"Transaction non trouvée")
    return doc

@app.get("/transactions/user/{user_id}")
def get_user_transactions(user_id: str,
                          limit: int = Query(50,ge=1,le=200),
                          days:  int = Query(30,ge=1,le=365)):
    since = (datetime.now()-timedelta(days=days)).isoformat()
    docs  = list(col_tx.find({"user_id":user_id,"timestamp":{"$gte":since}},
                              {"_id":0}).sort("timestamp",-1).limit(limit))
    fc = sum(1 for d in docs if d.get("is_fraud") in [1,True])
    ta = sum(d.get("amount",0) for d in docs)
    return {"user_id":user_id,"total":len(docs),"fraud_count":fc,
            "total_amount":round(ta,2),
            "fraud_rate":round(fc/len(docs)*100,1) if docs else 0,
            "transactions":docs}

@app.get("/stats/by-city")
def stats_by_city():
    since = since_24h()
    pipe = [
        {"$match":  {"timestamp":{"$gte":since}}},
        {"$group":  {"_id":"$city",
                     "total":      {"$sum":1},
                     "frauds":     {"$sum":{"$cond":[{"$eq":["$is_fraud",1]},1,0]}},
                     "avg_amount": {"$avg":"$amount"}}},
        {"$sort":   {"frauds":-1}},
        {"$limit":  20}
    ]
    res = list(col_tx.aggregate(pipe))
    return {"data":[
        {"city":       r["_id"],
         "total":      r["total"],
         "frauds":     r["frauds"],
         "fraud_rate": round(r["frauds"]/max(1,r["total"])*100,1),
         "avg_amount": round(r.get("avg_amount",0),2)}
        for r in res if r["_id"]
    ]}

@app.get("/stats/top-users")
def stats_top_users(limit: int = Query(10,ge=1,le=50)):
    since = since_24h()
    pipe = [
        {"$match":  {"is_fraud":1,"timestamp":{"$gte":since}}},
        {"$group":  {"_id":"$user_id",
                     "fraud_count":  {"$sum":1},
                     "total_amount": {"$sum":"$amount"},
                     "last_fraud":   {"$max":"$timestamp"}}},
        {"$sort":   {"fraud_count":-1}},
        {"$limit":  limit}
    ]
    res = list(col_tx.aggregate(pipe))
    return {"data":[
        {"user_id":      r["_id"],
         "fraud_count":  r["fraud_count"],
         "total_amount": round(r["total_amount"],2),
         "last_fraud":   r["last_fraud"]}
        for r in res
    ]}

@app.get("/stats/timeline")
def stats_timeline(days: int = Query(1,ge=1,le=90)):
    since = (datetime.now()-timedelta(days=days)).isoformat()
    pipe = [
        {"$match":     {"is_fraud":1,"timestamp":{"$gte":since}}},
        {"$addFields": {"date":{"$substr":["$timestamp",0,10]}}},
        {"$group":     {"_id":"$date",
                        "frauds": {"$sum":1},
                        "amount": {"$sum":"$amount"}}},
        {"$sort":      {"_id":1}}
    ]
    res = list(col_tx.aggregate(pipe))
    return {"period_days":days,"data":[
        {"date":r["_id"],"frauds":r["frauds"],
         "amount":round(r["amount"],2)}
        for r in res
    ]}
@app.get("/stats/fraud-types")
def stats_fraud_types():
    pipe = [
        {"$match": {"is_fraud": 1}},
        {"$group": {"_id": "$fraud_reason", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    res = list(col_tx.aggregate(pipe))
    mapping = {
        "high_amount": "Montant suspect",
        "foreign_location": "Localisation etrangere",
        "unknown_device": "Appareil inconnu",
        "high_frequency": "Frequence elevee",
        "unusual_hour": "Heure suspecte",
    }
    result = {}
    for r in res:
        key = r["_id"] or ""
        if not key:
            continue
        label = mapping.get(key, key)
        result[label] = r["count"]
    return result

@app.get("/users/{user_id}/risk")
def user_risk(user_id: str):
    total = col_tx.count_documents({"user_id":user_id})
    if total == 0:
        raise HTTPException(404,f"Utilisateur {user_id} non trouvé")
    frauds = col_tx.count_documents({"user_id":user_id,"is_fraud":1})
    fr = frauds/total*100
    pipe = [
        {"$match": {"user_id":user_id}},
        {"$group": {"_id":None,
                    "avg":   {"$avg":"$amount"},
                    "mx":    {"$max":"$amount"},
                    "tot":   {"$sum":"$amount"},
                    "last":  {"$max":"$timestamp"},
                    "first": {"$min":"$timestamp"}}}
    ]
    s  = list(col_tx.aggregate(pipe))
    s  = s[0] if s else {}
    rl = ("CRITIQUE" if fr>=30 else
          "ÉLEVÉ"   if fr>=15 else
          "MOYEN"   if fr>=5  else "FAIBLE")
    return {"user_id":user_id,"risk_level":rl,
            "fraud_rate_pct":     round(fr,2),
            "total_transactions": total,
            "fraud_count":        frauds,
            "avg_amount":         round(s.get("avg",0),2),
            "max_amount":         round(s.get("mx", 0),2),
            "total_spent":        round(s.get("tot",0),2),
            "first_transaction":  s.get("first"),
            "last_transaction":   s.get("last")}

@app.get("/search")
def search(q: str = Query(...,min_length=1),
           limit: int = Query(20,ge=1,le=100)):
    query = {"$or":[
        {"user_id":       {"$regex":q,"$options":"i"}},
        {"transaction_id":{"$regex":q,"$options":"i"}},
        {"city":          {"$regex":q,"$options":"i"}}
    ]}
    docs = list(col_tx.find(query,{"_id":0}).sort("timestamp",-1).limit(limit))
    return {"query":q,"count":len(docs),"data":docs}

@app.post("/simulate")
def simulate():
    import random
    cities_fr = ["Dubai","Paris","London","Madrid"]
    users = [f"USER_{i:04d}" for i in range(1,31)]
    inserted = 0
    for _ in range(5):
        tx = {
            "transaction_id": str(_uuid.uuid4()),
            "user_id":        random.choice(users),
            "amount":         round(random.uniform(5000,20000),2),
            "city":           random.choice(cities_fr),
            "device":         "unknown_device",
            "is_fraud":       1,
            "fraud_reason":   "foreign_location",
            "timestamp":      datetime.now().isoformat(),
            "currency":       "MAD",
            "merchant":       "Test",
        }
        col_tx.insert_one({**tx})
        tx.pop("_id",None)
        alert = {**tx,"reasons":["Localisation étrangère"]}
        col_alert.insert_one({**alert})
        alert.pop("_id",None)
        if redis_client:
            try:
                redis_client.lpush("fraud_alerts",
                    json.dumps({**alert,"fraud_reasons":["Localisation étrangère"]}))
                redis_client.ltrim("fraud_alerts",0,499)
            except: pass
        inserted += 1
    return {"simulated":inserted,"timestamp":datetime.now().isoformat()}

@app.get("/history")
def get_history(limit: int = Query(30,ge=1,le=365)):
    """Historique quotidien archivé"""
    col_history = db["history"]
    docs = list(col_history.find({},{"_id":0}).sort("date",-1).limit(limit))
    return {"count":len(docs),"data":docs}

@app.post("/reset-daily")
def reset_daily():
    """Reset quotidien — archive et vide les données de la veille."""
    now = datetime.now()
    date_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    since = (now - timedelta(hours=24)).isoformat()

    # Stats du jour
    total  = col_tx.count_documents({"timestamp":{"$gte":since}})
    frauds = col_tx.count_documents({"is_fraud":1,"timestamp":{"$gte":since}})
    taux   = round(frauds/max(1,total)*100,2)

    amt_pipe = [{"$match":{"is_fraud":1,"timestamp":{"$gte":since}}},
                {"$group":{"_id":None,"total":{"$sum":"$amount"},"avg":{"$avg":"$amount"},"mx":{"$max":"$amount"}}}]
    amt = list(col_tx.aggregate(amt_pipe)); a = amt[0] if amt else {}

    top_pipe = [{"$match":{"is_fraud":1,"timestamp":{"$gte":since}}},
                {"$group":{"_id":"$user_id","count":{"$sum":1},"total":{"$sum":"$amount"}}},
                {"$sort":{"count":-1}},{"$limit":10}]
    top_users = list(col_tx.aggregate(top_pipe))

    city_pipe = [{"$match":{"timestamp":{"$gte":since}}},
                 {"$group":{"_id":"$city","total":{"$sum":1},
                            "frauds":{"$sum":{"$cond":[{"$eq":["$is_fraud",1]},1,0]}}}},
                 {"$sort":{"frauds":-1}}]
    by_city = list(col_tx.aggregate(city_pipe))

    type_pipe = [{"$match":{"is_fraud":1,"timestamp":{"$gte":since}}},
                 {"$group":{"_id":"$fraud_reason","count":{"$sum":1}}},
                 {"$sort":{"count":-1}}]
    by_type = list(col_tx.aggregate(type_pipe))

    # Archive
    col_history = db["history"]
    # Eviter les doublons
    col_history.delete_many({"date":date_str})
    col_history.insert_one({
        "date": date_str,
        "archived_at": now.isoformat(),
        "summary": {
            "total_transactions": total,
            "total_frauds": frauds,
            "total_normals": max(0,total-frauds),
            "fraud_rate_pct": taux,
            "fraud_total_amount": round(a.get("total",0),2),
            "fraud_avg_amount":   round(a.get("avg",0),2),
            "fraud_max_amount":   round(a.get("mx",0),2),
        },
        "top_fraudsters": [{"user_id":r["_id"],"fraud_count":r["count"],"total_amount":round(r["total"],2)} for r in top_users],
        "by_city": [{"city":r["_id"],"total":r["total"],"frauds":r["frauds"]} for r in by_city if r["_id"]],
        "by_fraud_type": [{"type":r["_id"] or "Autre","count":r["count"]} for r in by_type],
    })

    # Supprimer les données de la veille (garder seulement 24h)
    cutoff = (now - timedelta(hours=24)).isoformat()
    del_tx = col_tx.delete_many({"timestamp":{"$lt":cutoff}})
    del_al = col_alert.delete_many({"timestamp":{"$lt":cutoff}})

    return {
        "status": "reset_done",
        "date": date_str,
        "archived": {"transactions":total,"frauds":frauds,"rate":taux},
        "deleted": {"transactions":del_tx.deleted_count,"alerts":del_al.deleted_count},
    }

if __name__=="__main__":
    import uvicorn
    uvicorn.run("main:app",host="0.0.0.0",port=8000,reload=True)
