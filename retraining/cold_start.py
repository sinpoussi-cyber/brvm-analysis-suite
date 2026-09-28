"""Entraînement initial (« cold start ») des titres nouvellement cotés.

Cas traité : une société est présente dans la table `companies` (ex. BBGC,
admise en sept. 2026) mais n'a pas encore de dossier `modeles/<TICKER>/`.
Le warm-start de retrain.py ne peut rien pour elle puisqu'il n'y a aucun
modèle à rafraîchir.

Dès que le titre dispose d'au moins MIN_HISTORY cotations, on entraîne un
modèle de zéro avec EXACTEMENT l'architecture standard du dépôt
(celle de SIBC, SNTS, etc.) :

    Input(20, 1) → GRU(64, return_sequences) → Dropout(0.2)
                 → GRU(32) → Dropout(0.2) → Dense(1)
    Adam(lr=1e-3), loss MSE, univarié (prix), horizon J+1, MinMaxScaler(0,1)

Procédure :
  1. Évaluation honnête : split chronologique 80/20, scaler ajusté sur le
     train uniquement, early stopping sur une validation interne → MAPE et R²
     calculés sur le test en FCFA.
  2. Modèle final : scaler réajusté sur tout l'historique, réentraînement sur
     tout l'historique avec le nombre d'époques retenu à l'étape 1.
  3. Sauvegarde dans modeles/<TICKER>/ :
        model_GRU.keras, scaler.pkl, params.json
     params.json est lu par prediction_analyzer.py (même structure que
     MODELS_PARAMS) : aucune modification manuelle n'est nécessaire.

Tant que l'historique est insuffisant, le titre est simplement signalé
« en attente (n/MIN_HISTORY cotations) » dans le mail mensuel.
"""
import json
import os
from datetime import date

import joblib
import numpy as np
import pandas as pd

LOOK_BACK = 20
UNITS_1, UNITS_2 = 64, 32
DROPOUT = 0.2
LR = 1e-3
MAX_EPOCHS = 100
PATIENCE = 10
MIN_FINAL_EPOCHS = 10  # plancher : évite un modèle final sous-entraîné si la validation s'arrête très tôt
BATCH = 16
TEST_RATIO = 0.2
MIN_HISTORY = 120      # ≈ 6 mois de séances : ~100 séquences d'entraînement
SEED = 42

# Seuils identiques à ceux de MODELS_PARAMS (mape_ok / r2_ok)
MAPE_OK = 5.0
R2_OK = 0.7


def _build_model():
    import keras
    from keras import layers

    keras.utils.set_random_seed(SEED)
    model = keras.Sequential([
        keras.Input(shape=(LOOK_BACK, 1)),
        layers.GRU(UNITS_1, return_sequences=True),
        layers.Dropout(DROPOUT),
        layers.GRU(UNITS_2),
        layers.Dropout(DROPOUT),
        layers.Dense(1),
    ])
    model.compile(optimizer=keras.optimizers.Adam(LR), loss="mse")
    return model


def _sequences(scaled):
    X, y = [], []
    for i in range(len(scaled) - LOOK_BACK):
        X.append(scaled[i:i + LOOK_BACK])
        y.append(scaled[i + LOOK_BACK, 0])
    return np.array(X), np.array(y)


def _prices(rows):
    df = pd.DataFrame(rows)
    if df.empty or "price" not in df.columns:
        return np.array([])
    p = pd.to_numeric(df["price"], errors="coerce").dropna()
    p = p[p > 0]
    return p.values.astype(float).reshape(-1, 1)


def train_new_model(ticker, rows, models_dir):
    """Entraîne et enregistre un premier modèle pour `ticker`.

    rows : cotations triées par date croissante (db.fetch_recent).
    Retourne un dict status ∈ {trained, waiting, error}.
    """
    from sklearn.preprocessing import MinMaxScaler
    import keras

    prices = _prices(rows)
    n = len(prices)
    if n < MIN_HISTORY:
        return {"status": "waiting", "n_obs": n,
                "reason": f"{n}/{MIN_HISTORY} cotations disponibles"}

    try:
        # ── 1. Évaluation hors échantillon ────────────────────────────────
        split = int(n * (1 - TEST_RATIO))
        train_raw = prices[:split]
        test_raw = prices[split - LOOK_BACK:]      # contexte pour la 1re séquence test

        sc_eval = MinMaxScaler((0, 1)).fit(train_raw)
        X_tr, y_tr = _sequences(sc_eval.transform(train_raw))
        X_te, y_te = _sequences(sc_eval.transform(test_raw))

        model = _build_model()
        early = keras.callbacks.EarlyStopping(
            monitor="val_loss", patience=PATIENCE, restore_best_weights=True)
        hist = model.fit(X_tr, y_tr, validation_split=0.1, shuffle=False,
                         epochs=MAX_EPOCHS, batch_size=BATCH, verbose=0,
                         callbacks=[early])
        best_epochs = max(int(np.argmin(hist.history["val_loss"]) + 1), MIN_FINAL_EPOCHS)

        pred = sc_eval.inverse_transform(model.predict(X_te, verbose=0)).ravel()
        act = sc_eval.inverse_transform(y_te.reshape(-1, 1)).ravel()
        mape = float(np.mean(np.abs(pred - act) / np.abs(act)) * 100)
        ss_res = float(np.sum((act - pred) ** 2))
        ss_tot = float(np.sum((act - act.mean()) ** 2))
        r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

        # ── 2. Modèle final sur tout l'historique ─────────────────────────
        scaler = MinMaxScaler((0, 1)).fit(prices)
        X_all, y_all = _sequences(scaler.transform(prices))
        final = _build_model()
        final.fit(X_all, y_all, shuffle=False, epochs=best_epochs,
                  batch_size=BATCH, verbose=0)

        # ── 3. Sauvegarde ─────────────────────────────────────────────────
        out_dir = os.path.join(models_dir, ticker)
        os.makedirs(out_dir, exist_ok=True)
        final.save(os.path.join(out_dir, "model_GRU.keras"))
        joblib.dump(scaler, os.path.join(out_dir, "scaler.pkl"))

        params = {
            "best_model": "GRU", "look_back": LOOK_BACK, "log_transform": False,
            "units": UNITS_1, "dropout": DROPOUT, "lr": LR,
            "mape_test": round(mape, 4), "r2_test": round(r2, 4),
            "mape_ok": mape < MAPE_OK, "r2_ok": r2 > R2_OK,
            "source": "base",
            "origin": "cold_start",
            "trained_on": date.today().isoformat(),
            "n_obs": n, "epochs": best_epochs,
        }
        with open(os.path.join(out_dir, "params.json"), "w", encoding="utf-8") as f:
            json.dump(params, f, indent=2, ensure_ascii=False)

        return {"status": "trained", "n_obs": n, "mape": mape, "r2": r2,
                "epochs": best_epochs}
    except Exception as exc:  # noqa: BLE001 — un rapport, pas un crash du run
        return {"status": "error", "n_obs": n, "reason": str(exc)}
