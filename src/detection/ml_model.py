"""
ml_model.py — Module d'intégration ML dans le détecteur temps réel
Charge le modèle entraîné et expose une fonction score_transaction()
Features alignées sur fraud_ml.py (9 features)
"""

import os
import numpy as np
import joblib

_BASE         = os.path.dirname(__file__)
_MODEL_PATH   = os.path.join(_BASE, "models", "fraud_model.pkl")
_SCALER_PATH  = os.path.join(_BASE, "models", "scaler.pkl")
_FEATURES_PATH = os.path.join(_BASE, "models", "features.pkl")

_model    = None
_scaler   = None
_features = None


def _load():
    global _model, _scaler, _features
    if _model is None:
        if not os.path.exists(_MODEL_PATH):
            raise FileNotFoundError(
                "Modèle non trouvé. Lancez d'abord notebooks/fraud_ml.py"
            )
        _model    = joblib.load(_MODEL_PATH)
        _scaler   = joblib.load(_SCALER_PATH)
        _features = joblib.load(_FEATURES_PATH)
        print(f"[ML] Modèle chargé depuis {_MODEL_PATH}")
        print(f"[ML] Features attendues : {_features}")


def score_transaction(transaction: dict) -> dict:
    """
    Calcule un score de fraude ML pour une transaction.
    Features identiques à fraud_ml.py (9 features).
    """
    _load()

    import pandas as pd
    from datetime import datetime

    # ── Feature engineering identique à fraud_ml.py ──
    ts = transaction.get("timestamp", "")
    try:
        dt = pd.to_datetime(str(ts))
        hour        = dt.hour
        day_of_week = dt.dayofweek
    except Exception:
        hour        = 12
        day_of_week = 0

    amount   = float(transaction.get("amount", 0))
    city     = str(transaction.get("city", "")).strip()
    device   = str(transaction.get("device", "")).strip()

    FOREIGN_CITIES = {"Dubai", "Paris", "London", "Madrid", "Istanbul"}

    # Fréquence utilisateur — non disponible transaction par transaction,
    # on utilise une valeur neutre (5 = médiane)
    user_tx_count = int(transaction.get("user_tx_count", 5))

    features = {
        "amount":           amount,
        "amount_log":       np.log1p(amount),
        "hour":             hour,
        "day_of_week":      day_of_week,
        "suspicious_hour":  1 if 1 <= hour <= 4 else 0,
        "foreign_location": 1 if city in FOREIGN_CITIES else 0,
        "unknown_device":   1 if device == "unknown_device" else 0,
        "high_amount":      1 if amount > 5000 else 0,
        "user_tx_count":    user_tx_count,
    }

    X    = pd.DataFrame([features])[_features]
    X_sc = _scaler.transform(X)
    score = float(_model.predict_proba(X_sc)[0][1])

    if score >= 0.8:
        confidence = "HIGH"
    elif score >= 0.5:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "fraud_score": round(score, 4),
        "is_fraud_ml": score >= 0.5,
        "confidence":  confidence,
    }


def is_model_available() -> bool:
    return os.path.exists(_MODEL_PATH)


if __name__ == "__main__":
    normal_tx = {
        "amount": 120, "timestamp": "2026-05-08T14:30:00",
        "city": "Casablanca", "device": "mobile_ios",
    }
    suspect_tx = {
        "amount": 8000, "timestamp": "2026-05-08T02:15:00",
        "city": "Dubai", "device": "unknown_device",
    }
    for tx, label in [(normal_tx, "Normale"), (suspect_tx, "Suspecte")]:
        result = score_transaction(tx)
        print(f"[{label}] Score={result['fraud_score']} | "
              f"Fraude={result['is_fraud_ml']} | "
              f"Confiance={result['confidence']}")