"""
Fix main.py : is_fraud stocké comme 1/0 (int) au lieu de True/False (bool)
"""
import re

path = r'C:\Users\dell\fraud-observatory\src\api\main.py'

with open(path, 'r', encoding='utf-8') as f:
    src = f.read()

print("Avant fix:")
matches = re.findall(r'.{15}is_fraud.{35}', src)
for m in set(matches):
    print(" ", repr(m))

# ── Fix 1: count_documents avec is_fraud: True/False ──
src = src.replace(
    '{"is_fraud": {"$in": [1, True]}}',
    '{"is_fraud": {"$in": [1, True]}}'
)
# Remplace les booléens purs restants
src = src.replace('"is_fraud": True',  '"is_fraud": {"$in": [1, True]}')
src = src.replace('"is_fraud": False', '"is_fraud": {"$in": [0, False]}')

# ── Fix 2: pipeline $cond ──
src = src.replace(
    '"$cond": ["$is_fraud", 1, 0]',
    '"$cond": [{"$in": ["$is_fraud", [1, True]]}, 1, 0]'
)

# ── Fix 3: query filter is_fraud dans get_transactions ──
# query["is_fraud"] = is_fraud  → doit supporter 1/True
old_q = 'if is_fraud  is not None: query["is_fraud"]  = is_fraud'
new_q = 'if is_fraud  is not None: query["is_fraud"]  = {"$in": [1, True]} if is_fraud else {"$in": [0, False]}'
src = src.replace(old_q, new_q)

# ── Fix 4: $match is_fraud dans aggregations ──
src = src.replace(
    '{"$match": {"is_fraud": True}}',
    '{"$match": {"is_fraud": {"$in": [1, True]}}}'
)
src = src.replace(
    '{"$match": {"is_fraud": False}}',
    '{"$match": {"is_fraud": {"$in": [0, False]}}}'
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(src)

print("\nApres fix:")
matches2 = re.findall(r'.{15}is_fraud.{50}', src)
for m in set(matches2):
    print(" ", repr(m))
print("\nDone — redémarre l'API !")