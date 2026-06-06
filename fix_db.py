from pymongo import MongoClient
db = MongoClient('mongodb://localhost:27017')['fraud_observatory']
r1 = db.transactions.update_many({'fraud_reason':'Montant suspect (>5000 MAD)'},{'$set':{'fraud_reason':'high_amount'}})
r2 = db.transactions.update_many({'fraud_reason':'Appareil inconnu'},{'$set':{'fraud_reason':'unknown_device'}})
print(f'Corrige: {r1.modified_count} + {r2.modified_count} documents')
