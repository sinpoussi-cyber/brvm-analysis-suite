"""Réestimation warm-start d'un modèle GRU par titre.

Principe : on RECHARGE le modèle et le scaler existants, puis on poursuit
l'entraînement (fine-tuning) à faible learning rate sur les 100 dernières
cotations. Les poids appris sur l'historique long sont conservés (mémoire longue),
on ne fait que les rafraîchir avec le récent.

Le scaler existant est RÉUTILISÉ (transform, pas fit) pour rester cohérent avec
les poids du modèle. C'est le comportement voulu par un warm-start ; si des prix
ont fortement dérivé, certaines valeurs mises à l'échelle peuvent sortir de [0,1]
(sans danger pour le fine-tuning). Voir README pour l'alternative refit.

>>> HYPOTHÈSE À VÉRIFIER (README, point 1) :
    FEATURES ci-dessous décrit l'ordre exact des variables d'entrée de tes modèles,
    avec la CIBLE (le prix) en première position (index 0). En univarié
    (n_features == 1 dans le modèle), seul `price` est utilisé et tout est automatique.
"""
import pickle
import numpy as np
import pandas as pd
import keras

# Ordre des colonnes candidates. `price` DOIT rester en tête (c'est la cible).
FEATURES = ["price", "volume", "value", "company_capitalization"]

MIN_SEQUENCES = 10   # nb minimal de séquences d'entraînement pour tenter un fine-tuning
FINE_TUNE_LR = 1e-4  # petit LR : on rafraîchit sans effacer la mémoire longue
EPOCHS = 25
BATCH = 16


def _build_matrix(recent_rows, n_features):
    df = pd.DataFrame(recent_rows)
    for c in FEATURES:
        if c not in df.columns:
            df[c] = 0.0
    df = df[FEATURES].astype(float).ffill().fillna(0.0)
    return df[FEATURES[:n_features]].values  # (T, n_features)


def _make_sequences(scaled, look_back, horizon, target_idx=0):
    X, y = [], []
    for i in range(len(scaled) - look_back - horizon + 1):
        X.append(scaled[i:i + look_back])
        y.append(scaled[i + look_back:i + look_back + horizon, target_idx])
    return np.array(X), np.array(y)


def warm_start_finetune(model_path, scaler_path, recent_rows):
    """Fine-tune le modèle en place. Retourne un dict de statut.

    status ∈ {retrained, skip, error}
    """
    try:
        model = keras.models.load_model(model_path)
        with open(scaler_path, "rb") as f:
            scaler = pickle.load(f)

        look_back = int(model.input_shape[1])
        n_features = int(model.input_shape[2])
        horizon = int(np.prod(model.output_shape[1:]))

        scaler_nf = int(getattr(scaler, "n_features_in_", n_features))
        if scaler_nf != n_features:
            return {"status": "skip",
                    "reason": f"scaler ({scaler_nf} features) ≠ modèle ({n_features})"}

        matrix = _build_matrix(recent_rows, n_features)
        needed = look_back + horizon + MIN_SEQUENCES
        if len(matrix) < needed:
            return {"status": "skip",
                    "reason": f"{len(matrix)} cotations (< {needed} requis pour L={look_back}, H={horizon})"}

        scaled = scaler.transform(matrix)
        X, y = _make_sequences(scaled, look_back, horizon, target_idx=0)
        y = y.reshape(len(y), horizon)
        if len(X) < MIN_SEQUENCES:
            return {"status": "skip", "reason": f"{len(X)} séquences (< {MIN_SEQUENCES})"}

        loss = getattr(model, "loss", None) or "mse"
        model.compile(optimizer=keras.optimizers.Adam(FINE_TUNE_LR), loss=loss)
        early = keras.callbacks.EarlyStopping(
            monitor="loss", patience=5, restore_best_weights=True
        )
        hist = model.fit(X, y, epochs=EPOCHS, batch_size=BATCH,
                         verbose=0, callbacks=[early])

        model.save(model_path)  # écrase le .keras du dossier du titre
        return {
            "status": "retrained",
            "samples": int(len(X)),
            "epochs": int(len(hist.history["loss"])),
            "final_loss": float(hist.history["loss"][-1]),
            "look_back": look_back,
            "horizon": horizon,
            "n_features": n_features,
        }
    except Exception as exc:  # noqa: BLE001 - on veut un rapport, pas un crash du run
        return {"status": "error", "reason": str(exc)}
