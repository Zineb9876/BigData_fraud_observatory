"""
Real-Time Fraud Observatory — Module ML v2
Entraînement Random Forest + XGBoost aligné sur les vrais champs MongoDB
"""

import pymongo
import pandas as pd
import numpy as np
import joblib
import os
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("  REAL-TIME FRAUD OBSERVATORY — ENTRAÎNEMENT ML v2")
print("=" * 60)

# ── 1. CHARGEMENT ─────────────────────────────────────────────────────────────
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["fraud_observatory"]

print("\n📦 Chargement des données depuis MongoDB...")
transactions = list(db.transactions.find({}, {"_id": 0}))
print(f"✅ {len(transactions)} transactions chargées")

if len(transactions) < 100:
    print("⚠️  Pas assez de données. Lancez le simulateur plusieurs fois.")
    client.close()
    exit(1)

df = pd.DataFrame(transactions)
print(f"📊 Colonnes : {list(df.columns)}")

# ── 2. NETTOYAGE ──────────────────────────────────────────────────────────────
df['is_fraud'] = df['is_fraud'].astype(int)

print(f"\nDistribution fraude :")
vc = df['is_fraud'].value_counts()
print(vc)
fraud_rate = vc.get(1, 0) / len(df) * 100
print(f"Taux de fraude : {fraud_rate:.1f}%")

if fraud_rate > 40:
    print("\n⚠️  Taux de fraude élevé (données biaisées).")
    print("   Conseil : effacez MongoDB et régénérez avec le nouveau simulateur.")
    print("   Continuer avec les données actuelles...\n")

# ── 3. FEATURE ENGINEERING ────────────────────────────────────────────────────
print("🔧 Création des features...")

FOREIGN_CITIES = {"Dubai", "Paris", "London", "Madrid", "Istanbul"}

def parse_hour(ts):
    try:
        return pd.to_datetime(ts).hour
    except:
        return 12

def parse_dow(ts):
    try:
        return pd.to_datetime(ts).dayofweek
    except:
        return 0

df['hour']            = df['timestamp'].apply(parse_hour)
df['day_of_week']     = df['timestamp'].apply(parse_dow)
df['amount']          = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
df['amount_log']      = np.log1p(df['amount'])
df['suspicious_hour'] = df['hour'].apply(lambda h: 1 if 1 <= h <= 4 else 0)
df['foreign_location']= df['city'].apply(lambda c: 1 if c in FOREIGN_CITIES else 0)
df['unknown_device']  = df['device'].apply(lambda d: 1 if d == 'unknown_device' else 0)
df['high_amount']     = df['amount'].apply(lambda a: 1 if a > 5000 else 0)
user_freq             = df.groupby('user_id')['transaction_id'].count()
df['user_tx_count']   = df['user_id'].map(user_freq)

FEATURES = [
    'amount', 'amount_log', 'hour', 'day_of_week',
    'suspicious_hour', 'foreign_location', 'unknown_device',
    'high_amount', 'user_tx_count'
]

X = df[FEATURES].fillna(0)
y = df['is_fraud']

print(f"✅ {len(FEATURES)} features : {FEATURES}")
print(f"   Fraudes={y.sum()} | Normales={(y==0).sum()}")

# ── 4. SPLIT ──────────────────────────────────────────────────────────────────
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

from imblearn.over_sampling import SMOTE

minority = min(y_train.sum(), (y_train == 0).sum())
if minority >= 6 and fraud_rate < 40:
    sm = SMOTE(random_state=42, k_neighbors=min(5, int(minority) - 1))
    X_train_res, y_train_res = sm.fit_resample(X_train, y_train)
    print(f"\n⚖️  SMOTE appliqué : {len(X_train_res)} samples")
else:
    X_train_res, y_train_res = X_train, y_train
    print("\nℹ️  Pas de SMOTE")

scaler = StandardScaler()
X_train_sc = scaler.fit_transform(X_train_res)
X_test_sc  = scaler.transform(X_test)

# ── 5. ENTRAÎNEMENT ───────────────────────────────────────────────────────────
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, accuracy_score

print("\n" + "=" * 60)
print("🌲 ENTRAÎNEMENT RANDOM FOREST")
print("=" * 60)

rf = RandomForestClassifier(
    n_estimators=300, max_depth=12,
    class_weight='balanced', random_state=42, n_jobs=-1
)
rf.fit(X_train_sc, y_train_res)
rf_pred  = rf.predict(X_test_sc)
rf_proba = rf.predict_proba(X_test_sc)[:, 1]
rf_auc   = roc_auc_score(y_test, rf_proba)

print(f"✅ AUC-ROC  : {rf_auc:.4f}")
print(f"   Accuracy : {accuracy_score(y_test, rf_pred):.4f}")
print(classification_report(y_test, rf_pred, target_names=['Normal', 'Fraude']))

print("=" * 60)
try:
    from xgboost import XGBClassifier
    ratio = (y_train_res == 0).sum() / max((y_train_res == 1).sum(), 1)
    xgb = XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.05,
        scale_pos_weight=ratio, subsample=0.8, colsample_bytree=0.8,
        random_state=42, eval_metric='logloss', verbosity=0
    )
    label = "XGBoost"
except ImportError:
    from sklearn.ensemble import GradientBoostingClassifier
    xgb = GradientBoostingClassifier(n_estimators=200, max_depth=5, random_state=42)
    label = "GradientBoosting"

print(f"⚡ ENTRAÎNEMENT {label}")
print("=" * 60)
xgb.fit(X_train_sc, y_train_res)
xgb_pred  = xgb.predict(X_test_sc)
xgb_proba = xgb.predict_proba(X_test_sc)[:, 1]
xgb_auc   = roc_auc_score(y_test, xgb_proba)

print(f"✅ AUC-ROC  : {xgb_auc:.4f}")
print(f"   Accuracy : {accuracy_score(y_test, xgb_pred):.4f}")
print(classification_report(y_test, xgb_pred, target_names=['Normal', 'Fraude']))

# ── 6. MEILLEUR MODÈLE ────────────────────────────────────────────────────────
best_model = rf if rf_auc >= xgb_auc else xgb
best_name  = "RandomForest" if rf_auc >= xgb_auc else label
best_auc   = max(rf_auc, xgb_auc)

print("=" * 60)
print(f"🏆 MEILLEUR MODÈLE : {best_name}  (AUC = {best_auc:.4f})")
print("=" * 60)

fi = pd.Series(best_model.feature_importances_, index=FEATURES).sort_values(ascending=False)
print("\n📊 Importance des features :")
for feat, imp in fi.items():
    bar = "█" * int(imp * 40)
    print(f"   {feat:<22} {bar} {imp:.3f}")

# ── 7. SAUVEGARDE ─────────────────────────────────────────────────────────────
models_dir = os.path.join(os.path.dirname(__file__), '..', 'src', 'detection', 'models')
os.makedirs(models_dir, exist_ok=True)

joblib.dump(best_model, os.path.join(models_dir, 'fraud_model.pkl'))
joblib.dump(scaler,     os.path.join(models_dir, 'scaler.pkl'))
joblib.dump(FEATURES,   os.path.join(models_dir, 'features.pkl'))

print(f"\n✅ Modèle sauvegardé dans {models_dir}")

# ── 8. TEST RAPIDE ────────────────────────────────────────────────────────────
print("\n🧪 TEST SUR EXEMPLES")
print("-" * 50)

examples = [
    {"amount": 120,  "amount_log": np.log1p(120),  "hour": 14, "day_of_week": 1,
     "suspicious_hour": 0, "foreign_location": 0, "unknown_device": 0,
     "high_amount": 0, "user_tx_count": 5,
     "label": "Normale  — 120 MAD, Casablanca, 14h, mobile"},
    {"amount": 8000, "amount_log": np.log1p(8000), "hour": 2,  "day_of_week": 6,
     "suspicious_hour": 1, "foreign_location": 1, "unknown_device": 1,
     "high_amount": 1, "user_tx_count": 1,
     "label": "Suspecte — 8000 MAD, Paris, 2h, appareil inconnu"},
    {"amount": 350,  "amount_log": np.log1p(350),  "hour": 3,  "day_of_week": 0,
     "suspicious_hour": 1, "foreign_location": 0, "unknown_device": 0,
     "high_amount": 0, "user_tx_count": 8,
     "label": "Ambigue  — 350 MAD, Rabat, 3h du matin"},
]

for ex in examples:
    lbl = ex.pop("label")
    fv   = pd.DataFrame([ex])[FEATURES]
    fv_sc = scaler.transform(fv)
    score = best_model.predict_proba(fv_sc)[0][1]
    verdict = "FRAUDE" if score > 0.5 else "NORMAL"
    print(f"[{verdict}] Score={score:.3f} | {lbl}")

print("\n✅ Entraînement terminé ! Modèle prêt pour fraud_detector.py")
client.close()