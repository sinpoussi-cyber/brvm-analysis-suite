# ==============================================================================
# MODULE: PREDICTION ANALYZER V17.0 — BRVM 47 ACTIONS
# ------------------------------------------------------------------------------
# NOUVEAUTÉS V17.0 :
#
#   1. LIMITE BRVM ±7.5%/jour (réglementaire)
#      Seule la borne JOURNALIÈRE est appliquée : chaque prédiction J+n ne peut
#      dépasser ±7.5% de la prédiction J+(n-1). La borne cumulée a été retirée
#      car elle empêcherait les tendances légitimes sur plusieurs jours.
#
#   2. INTERVALLES DE CONFIANCE — diffusion brownienne (IC 90%)
#      Formule : σ_rel × √n × 1.65 × pred
#        - σ_rel   : volatilité journalière relative (écart-type des rendements
#                    sur les 30 derniers jours disponibles)
#        - √n      : diffusion brownienne — l'incertitude croît comme la racine
#                    du temps, pas linéairement. À J+4 l'incertitude est ×2,
#                    pas ×4.
#        - 1.65    : quantile 95% de la loi normale → IC à 90%
#                    (5% de chance que le vrai cours soit en dehors de l'intervalle
#                    de chaque côté)
#        L'intervalle est plafonné par la limite BRVM journalière cumulée
#        (7.5% × n) pour rester cohérent avec la réglementation.
#
#   3. CONFIANCE DYNAMIQUE
#      Combine 4 critères : mape_ok, r2_ok, vol_ok (< 2%/j), flat_ok.
#      Les modèles avec prédictions plates reçoivent automatiquement "Faible".
#
# Types de modèles gérés :
#   - GRU    : Input → GRU(64,rs=True) → Dropout → GRU(32,rs=False) → Dropout → Dense(1)
#   - GRU adv: Input → GRU(128,rs=True) → Dropout → GRU(64,rs=False) → Dropout → Dense(1)
#   - LSTM   : Input → LSTM(64,rs=True) → Dropout → LSTM(32,rs=False) → Dropout → Dense(1)
#   - BiGRU  : Input → Bidir(GRU,rs=True) → Drop → Bidir(GRU,rs=False) → Drop → Dense(16) → Dense(1)
#
# Structure fichiers :
#   source "base"     (42 symboles) : model_*.keras + scaler.pkl
#   source "advanced" ( 5 symboles) : model_*_advanced.keras + scaler_advanced.pkl
# ==============================================================================

import os
import json
import logging
import zipfile
import tempfile
import joblib
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import (
    GRU, LSTM, Dense, Dropout, Bidirectional, Input
)
from tensorflow.keras.optimizers import Adam

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Variables d'environnement — connexion PostgreSQL
# ---------------------------------------------------------------------------
DB_NAME     = os.environ.get("DB_NAME")
DB_USER     = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST     = os.environ.get("DB_HOST")
DB_PORT     = os.environ.get("DB_PORT")

MODELS_DIR          = os.environ.get("MODELS_DIR", "./modeles")
HISTORIQUE_JOURS    = 100
NB_JOURS_PREDICTION = 10

# Limite réglementaire BRVM : variation max ±7.5% par jour
BRVM_DAILY_LIMIT = 0.075

_models_cache: dict = {}


# ==============================================================================
# CALENDRIER BRVM — JOURS FÉRIÉS 2026 (Côte d'Ivoire)
# ==============================================================================
JOURS_FERIES = {
    date(2026,  1,  1),
    date(2026,  3, 17),
    date(2026,  3, 20),
    date(2026,  4,  6),
    date(2026,  5,  1),
    date(2026,  5, 14),
    date(2026,  5, 27),
    date(2026,  6, 25),
    date(2026,  8,  7),
    date(2026,  8, 15),
    date(2026,  8, 26),
    date(2026, 11,  1),
    date(2026, 11, 15),
    date(2026, 12, 25),
}


def est_jour_ouvrable(d: date) -> bool:
    if isinstance(d, datetime):
        d = d.date()
    return d.weekday() <= 4 and d not in JOURS_FERIES


def prochains_jours_ouvrables(last_date: date, num_days: int = 10) -> list:
    if isinstance(last_date, datetime):
        last_date = last_date.date()
    result, current = [], last_date + timedelta(days=1)
    while len(result) < num_days:
        if est_jour_ouvrable(current):
            result.append(current)
        current += timedelta(days=1)
    return result


# ==============================================================================
# PARAMÈTRES DES 47 MODÈLES
# ==============================================================================
MODELS_PARAMS = {
    "ABJC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.3029,  "r2_test": 0.9702,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "BICB":  {"best_model": "GRU",   "look_back": 40, "log_transform": False,
               "units": 128, "dropout": 0.3, "lr": 0.001,
               "mape_test": 0.5486,  "r2_test": 0.026,
               "mape_ok": True,  "r2_ok": False, "source": "advanced"},
    "BICC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.228,   "r2_test": 0.9604,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "BNBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.6073,  "r2_test": 0.8689,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "BOAB":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2835,  "r2_test": 0.9786,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "BOABF": {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.4895,  "r2_test": 0.9334,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "BOAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2125,  "r2_test": 0.8286,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "BOAM":  {"best_model": "GRU",   "look_back": 60, "log_transform": False,
               "units": 128, "dropout": 0.3, "lr": 0.001,
               "mape_test": 1.2732,  "r2_test": 0.919,
               "mape_ok": True,  "r2_ok": True,  "source": "advanced"},
    "BOAN":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.9249,  "r2_test": 0.4895,
               "mape_ok": True,  "r2_ok": False, "source": "base"},
    "BOAS":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2579,  "r2_test": 0.9136,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "CABC":  {"best_model": "LSTM",  "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.297,   "r2_test": 0.9698,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "CBIBF": {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.8918,  "r2_test": 0.9489,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "CFAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 4.9873,  "r2_test": 0.9171,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "CIEC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2747,  "r2_test": 0.9283,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "ECOC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.6051,  "r2_test": 0.9663,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "ETIT":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.8994,  "r2_test": 0.9016,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "FTSC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.3064,  "r2_test": 0.97,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "LNBB":  {"best_model": "BiGRU", "look_back": 60, "log_transform": False,
               "units": 64,  "dropout": 0.3, "lr": 0.001,
               "mape_test": 0.8472,  "r2_test": 0.808,
               "mape_ok": True,  "r2_ok": True,  "source": "advanced"},
    "NEIC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.304,   "r2_test": 0.9577,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "NSBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.1638,  "r2_test": 0.9502,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "NTLC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.2438,  "r2_test": 0.7337,
               "mape_ok": True,  "r2_ok": False, "source": "base"},
    "ONTBF": {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.3025,  "r2_test": 0.8338,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "ORAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.1112,  "r2_test": 0.6873,
               "mape_ok": True,  "r2_ok": False, "source": "base"},
    "ORGT":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.0779,  "r2_test": 0.9122,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "PALC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.7197,  "r2_test": 0.8472,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "PRSC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.5991,  "r2_test": 0.9415,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SAFC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 6.389,   "r2_test": 0.9407,
               "mape_ok": False, "r2_ok": True,  "source": "base"},
    "SCRC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.2593,  "r2_test": 0.908,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SDCC":  {"best_model": "BiGRU", "look_back": 60, "log_transform": False,
               "units": 128, "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.894,   "r2_test": 0.7271,
               "mape_ok": True,  "r2_ok": False, "source": "advanced"},
    "SDSC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.6092,  "r2_test": 0.8197,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SEMC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.0298,  "r2_test": 0.9278,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SGBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.4225,  "r2_test": 0.864,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SHEC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.5238,  "r2_test": 0.9357,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SIBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.3876,  "r2_test": 0.9345,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SICC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.6798,  "r2_test": 0.6157,
               "mape_ok": True,  "r2_ok": False, "source": "base"},
    "SIVC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 5.8402,  "r2_test": 0.9471,
               "mape_ok": False, "r2_ok": True,  "source": "base"},
    "SLBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.1569,  "r2_test": 0.9713,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SMBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.6586,  "r2_test": 0.8521,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SNTS":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.8219,  "r2_test": 0.7655,
               "mape_ok": True,  "r2_ok": False, "source": "base"},
    "SOGC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.3089,  "r2_test": 0.8368,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "SPHC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.5344,  "r2_test": 0.8196,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "STAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.9611,  "r2_test": 0.9398,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
    "STBC":  {"best_model": "BiGRU", "look_back": 60, "log_transform": False,
               "units": 128, "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.4802,  "r2_test": 0.6015,
               "mape_ok": True,  "r2_ok": False, "source": "advanced"},
    "TTLC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.0205,  "r2_test": 0.6746,
               "mape_ok": True,  "r2_ok": False, "source": "base"},
    "TTLS":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.9356,  "r2_test": 0.7746,
               "mape_ok": True,  "r2_ok": False, "source": "base"},
    "UNLC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 5.7498,  "r2_test": 0.9566,
               "mape_ok": False, "r2_ok": True,  "source": "base"},
    "UNXC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 4.6783,  "r2_test": 0.9245,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
}


# ==============================================================================
# RÉSOLUTION DES CHEMINS (keras + scaler) SELON "source"
# ==============================================================================

def _resolve_paths(symbol: str):
    action_dir = os.path.join(MODELS_DIR, symbol)
    if not os.path.isdir(action_dir):
        logging.warning(f"⚠️  {symbol} : dossier absent ({action_dir})")
        return None, None

    source      = MODELS_PARAMS.get(symbol, {}).get("source", "base")
    all_files   = os.listdir(action_dir)
    keras_files = [f for f in all_files if f.endswith(".keras")]

    if not keras_files:
        logging.warning(f"⚠️  {symbol} : aucun .keras dans {action_dir}")
        return None, None

    if source == "advanced":
        candidates = [f for f in keras_files if "_advanced" in f]
    else:
        candidates = [f for f in keras_files if "_advanced" not in f]

    keras_file  = sorted(candidates)[0] if candidates else sorted(keras_files)[0]
    keras_path  = os.path.join(action_dir, keras_file)
    scaler_name = "scaler_advanced.pkl" if source == "advanced" else "scaler.pkl"
    scaler_path = os.path.join(action_dir, scaler_name)

    if not os.path.exists(scaler_path):
        logging.warning(f"⚠️  {symbol} : {scaler_name} absent dans {action_dir}")
        return None, None

    return keras_path, scaler_path


# ==============================================================================
# CHARGEMENT KERAS 3 → TF2
# ==============================================================================

def _build_model_from_config(config: dict) -> tf.keras.Model:
    """
    Reconstruit un modèle TF2/Keras 2 depuis un config.json Keras 3.
    Gère : Sequential GRU, Sequential LSTM, Sequential BiGRU.
    """
    layers_cfg  = config["config"]["layers"]
    layer_names = [l["class_name"] for l in layers_cfg]
    has_bidir   = "Bidirectional" in layer_names
    has_lstm    = "LSTM" in layer_names

    input_cfg   = layers_cfg[0]["config"]
    batch_shape = input_cfg["batch_shape"]
    look_back   = batch_shape[1]

    model = Sequential()
    model.add(Input(shape=(look_back, 1)))

    if has_bidir:
        for l in layers_cfg[1:]:
            cls = l["class_name"]
            cfg = l["config"]
            if cls == "Bidirectional":
                inner_cfg  = cfg["layer"]["config"]
                inner_units = inner_cfg["units"]
                return_seq  = inner_cfg["return_sequences"]
                model.add(Bidirectional(GRU(inner_units, return_sequences=return_seq)))
            elif cls == "Dropout":
                model.add(Dropout(cfg["rate"]))
            elif cls == "Dense":
                model.add(Dense(cfg["units"], activation=cfg.get("activation", "linear")))
    elif has_lstm:
        for l in layers_cfg[1:]:
            cls = l["class_name"]
            cfg = l["config"]
            if cls == "LSTM":
                model.add(LSTM(cfg["units"], return_sequences=cfg["return_sequences"]))
            elif cls == "Dropout":
                model.add(Dropout(cfg["rate"]))
            elif cls == "Dense":
                model.add(Dense(cfg["units"], activation=cfg.get("activation", "linear")))
    else:
        for l in layers_cfg[1:]:
            cls = l["class_name"]
            cfg = l["config"]
            if cls == "GRU":
                model.add(GRU(cfg["units"], return_sequences=cfg["return_sequences"]))
            elif cls == "Dropout":
                model.add(Dropout(cfg["rate"]))
            elif cls == "Dense":
                model.add(Dense(cfg["units"], activation=cfg.get("activation", "linear")))

    model.compile(optimizer=Adam(learning_rate=1e-3), loss="mse")
    return model


def load_keras3_model(keras_path: str, symbol: str) -> tf.keras.Model | None:
    """
    Charge un fichier .keras (format Keras 3 ZIP) dans TF2.
    Tente 3 méthodes de chargement des poids successivement.
    """
    try:
        if not zipfile.is_zipfile(keras_path):
            logging.error(f"❌ {symbol} : {os.path.basename(keras_path)} n'est pas un ZIP Keras 3")
            return None

        with zipfile.ZipFile(keras_path, "r") as zf:
            config = json.loads(zf.read("config.json"))
            names  = zf.namelist()

            with tempfile.TemporaryDirectory() as tmp:
                if "model.weights.h5" not in names:
                    logging.error(f"❌ {symbol} : model.weights.h5 absent dans le ZIP")
                    return None
                zf.extract("model.weights.h5", tmp)
                weights_path = os.path.join(tmp, "model.weights.h5")

                model = _build_model_from_config(config)
                logging.info(f"   ✓ {symbol} : architecture reconstruite ({model.count_params()} params)")

                try:
                    model.load_weights(weights_path)
                    logging.info(f"   ✓ {symbol} : poids chargés (standard)")
                    return model
                except Exception as e1:
                    logging.warning(f"   ↳ load_weights standard échoué : {str(e1)[:100]}")

                try:
                    model.load_weights(weights_path, by_name=True)
                    logging.info(f"   ✓ {symbol} : poids chargés (by_name=True)")
                    return model
                except Exception as e2:
                    logging.warning(f"   ↳ load_weights by_name échoué : {str(e2)[:100]}")

                try:
                    import h5py
                    with h5py.File(weights_path, "r") as hf:
                        for layer in model.layers:
                            if layer.name in hf:
                                grp   = hf[layer.name]
                                w_list = [grp[k][()] for k in grp.keys()]
                                if w_list:
                                    layer.set_weights(w_list)
                    logging.info(f"   ✓ {symbol} : poids chargés (h5py couche par couche)")
                    return model
                except Exception as e3:
                    logging.error(f"   ↳ h5py couche par couche échoué : {str(e3)[:100]}")

    except Exception as e:
        logging.error(f"❌ {symbol} : erreur load_keras3_model — {e}")

    return None


# ==============================================================================
# CONNEXION BASE DE DONNÉES
# ==============================================================================

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT,
        )
        logging.info("✅ Connexion PostgreSQL réussie")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur connexion DB : {e}")
        return None


# ==============================================================================
# CHARGEMENT DU MODÈLE ET DU SCALER (avec cache)
# ==============================================================================

def load_action_model(symbol: str):
    """Retourne (model, scaler) depuis le cache ou le disque."""
    if symbol in _models_cache:
        return _models_cache[symbol]

    keras_path, scaler_path = _resolve_paths(symbol)
    if keras_path is None:
        return None, None

    logging.info(f"📥 {symbol} : chargement de {os.path.basename(keras_path)}")

    model = load_keras3_model(keras_path, symbol)
    if model is None:
        logging.error(f"❌ {symbol} : modèle non chargeable")
        return None, None

    try:
        scaler = joblib.load(scaler_path)
        logging.info(f"   ✓ {symbol} : scaler chargé ({os.path.basename(scaler_path)})")
    except Exception as e:
        logging.error(f"❌ {symbol} : erreur chargement scaler — {e}")
        return None, None

    p = MODELS_PARAMS.get(symbol, {})
    logging.info(
        f"   ✅ {symbol} | {p.get('best_model','?')} | "
        f"look_back={p.get('look_back','?')} | "
        f"MAPE={p.get('mape_test','?')}% | R²={p.get('r2_test','?')}"
    )

    _models_cache[symbol] = (model, scaler)
    return model, scaler


# ==============================================================================
# PRÉDICTION — CŒUR DU MODULE
# ==============================================================================

def predire_10_jours(prices: pd.Series, dates: pd.Series, symbol: str):
    """
    Prédit les NB_JOURS_PREDICTION prochains jours ouvrables pour un symbole.

    Pipeline :
      1. Prédiction itérative auto-régressive via le modèle GRU/LSTM/BiGRU
      2. Application de la limite BRVM ±7.5%/jour (borne journalière uniquement)
      3. Calcul des intervalles de confiance par diffusion brownienne (IC 90%)
      4. Évaluation dynamique du niveau de confiance

    Retourne un dict avec dates, prédictions, bornes IC, confiance.
    """
    params = MODELS_PARAMS.get(symbol)
    if not params:
        logging.error(f"❌ {symbol} : introuvable dans MODELS_PARAMS")
        return None

    model, scaler = load_action_model(symbol)
    if model is None:
        return None

    look_back     = params["look_back"]
    log_transform = params["log_transform"]

    arr = np.array(prices.values, dtype=float)
    if len(arr) < look_back:
        logging.error(f"❌ {symbol} : {len(arr)} jours < look_back={look_back}")
        return None

    last_price = float(arr[-1])
    raw_date   = dates.iloc[-1]
    last_date  = raw_date.date() if isinstance(raw_date, datetime) else raw_date
    future_dates = prochains_jours_ouvrables(last_date, NB_JOURS_PREDICTION)

    # --- Normalisation ---
    sequence = arr[-look_back:].copy()
    if log_transform:
        sequence = np.log1p(sequence)

    try:
        seq_scaled = scaler.transform(sequence.reshape(-1, 1))
    except Exception as e:
        logging.error(f"❌ {symbol} : normalisation échouée — {e}")
        return None

    # --- Prédiction itérative auto-régressive ---
    current_seq  = seq_scaled.copy()
    preds_scaled = []

    for step in range(NB_JOURS_PREDICTION):
        x_input = current_seq[-look_back:].reshape(1, look_back, 1)
        try:
            p_val = float(model.predict(x_input, verbose=0)[0, 0])
        except Exception as e:
            logging.error(f"❌ {symbol} : erreur prédiction étape {step + 1} — {e}")
            return None
        preds_scaled.append(p_val)
        current_seq = np.vstack([current_seq, [[p_val]]])

    try:
        pred_raw = scaler.inverse_transform(
            np.array(preds_scaled).reshape(-1, 1)
        ).flatten()
    except Exception as e:
        logging.error(f"❌ {symbol} : dénormalisation échouée — {e}")
        return None

    predictions = np.expm1(pred_raw) if log_transform else pred_raw

    # ==========================================================================
    # ÉTAPE 1 — LIMITE BRVM : ±7.5% de variation maximale par jour (journalière)
    #
    # Règle réglementaire BRVM : une action ne peut varier de plus de ±7.5%
    # par rapport à son cours de la veille.
    #
    # Application : à chaque pas n, la prédiction est clippée par rapport à la
    # prédiction du pas précédent (ou last_price pour J+1).
    #   J+1 : clip(pred, last_price × 0.925, last_price × 1.075)
    #   J+2 : clip(pred, pred(J+1)  × 0.925, pred(J+1)  × 1.075)
    #   J+n : clip(pred, pred(J+n-1)× 0.925, pred(J+n-1)× 1.075)
    #
    # NOTE : seule la borne journalière est utilisée. Aucune borne cumulée
    # n'est appliquée, afin de permettre les tendances légitimes sur plusieurs jours.
    # ==========================================================================
    predictions_brvm = []
    prev_price = last_price

    for i, pred in enumerate(predictions):
        lower = prev_price * (1.0 - BRVM_DAILY_LIMIT)
        upper = prev_price * (1.0 + BRVM_DAILY_LIMIT)
        pred_clipped = float(np.clip(pred, lower, upper))

        if pred_clipped != pred:
            logging.debug(
                f"   BRVM clip {symbol} J+{i+1} : {pred:.2f} → {pred_clipped:.2f} "
                f"(bornes [{lower:.2f} ; {upper:.2f}])"
            )

        predictions_brvm.append(pred_clipped)
        prev_price = pred_clipped  # J+2 repart de la prédiction clippée de J+1

    predictions = np.array(predictions_brvm)

    # ==========================================================================
    # ÉTAPE 2 — INTERVALLES DE CONFIANCE (IC 90%) — diffusion brownienne
    #
    # Principe : un cours boursier suit un mouvement brownien. L'incertitude
    # sur la position future croît non pas linéairement mais comme √n.
    # Si la volatilité journalière est σ, à l'horizon n jours l'écart-type
    # de la distribution des cours futurs est σ × √n.
    #
    # Formule :
    #   sigma_n = pred(J+n) × σ_rel × √n × 1.65
    #
    #   - pred(J+n)  : prédiction centrale après clipping BRVM
    #   - σ_rel      : volatilité journalière relative (std des rendements
    #                  journaliers sur les 30 derniers jours), ex: 0.012 = 1.2%/j
    #   - √n         : diffusion brownienne (à J+4 l'incertitude est ×2, pas ×4)
    #   - 1.65       : quantile z à 95% de la loi normale standard
    #                  → intervalle à 90% bilatéral (5% de chaque côté)
    #
    # Plafond : l'intervalle est plafonné à BRVM_DAILY_LIMIT × n × pred
    # pour rester cohérent avec la réglementation (l'IC ne peut pas suggérer
    # une fourchette plus large que ce que la règle autorise physiquement).
    # ==========================================================================
    n_recent = min(30, len(arr) - 1)
    if n_recent > 0:
        daily_returns = np.diff(arr[-n_recent - 1:]) / arr[-n_recent - 1:-1]
        daily_returns = daily_returns[np.isfinite(daily_returns)]
        volatilite_rel = float(np.std(daily_returns)) if len(daily_returns) > 1 else 0.02
    else:
        volatilite_rel = 0.02  # fallback : 2%/j

    lower_bounds, upper_bounds = [], []
    for i, pred in enumerate(predictions):
        n = i + 1
        # Écart-type de l'IC à l'horizon n (diffusion brownienne)
        sigma_n = pred * volatilite_rel * (n ** 0.5) * 1.65
        # Plafond réglementaire : l'intervalle ne peut dépasser 7.5% × n
        brvm_max = pred * BRVM_DAILY_LIMIT * n
        sigma_n  = min(sigma_n, brvm_max)
        lower_bounds.append(float(max(0.0, pred - sigma_n)))
        upper_bounds.append(float(pred + sigma_n))

    # ==========================================================================
    # ÉTAPE 3 — CONFIANCE DYNAMIQUE
    #
    # 4 critères combinés (le plus pessimiste l'emporte) :
    #   mape_ok  : MAPE test < 5%  (précision du modèle sur données test)
    #   r2_ok    : R² test  > 0.7  (capacité du modèle à expliquer la variance)
    #   vol_ok   : σ_rel    < 2%/j (action stable, peu volatile)
    #   flat_ok  : range prédictions > 0.5% du cours (le modèle prédit une
    #              vraie dynamique, pas une valeur constante)
    # ==========================================================================
    both_ok    = params["mape_ok"] and params["r2_ok"]
    vol_ok     = volatilite_rel < 0.02
    pred_range = float(np.max(predictions)) - float(np.min(predictions))
    flat_ok    = (pred_range / last_price) > 0.005 if last_price > 0 else False

    def _niveau(j_idx: int) -> str:
        j = j_idx + 1
        if both_ok and vol_ok and flat_ok:
            return "Élevée"  if j <= 3 else "Moyenne"
        if both_ok and (vol_ok or flat_ok):
            return "Moyenne" if j <= 3 else "Faible"
        if params["mape_ok"] and vol_ok:
            return "Moyenne" if j <= 2 else "Faible"
        return "Faible"

    confidence_par_jour = [_niveau(i) for i in range(NB_JOURS_PREDICTION)]

    variation_pct = (
        ((float(predictions[-1]) - last_price) / last_price) * 100
        if last_price else 0.0
    )

    if both_ok and vol_ok and flat_ok and abs(variation_pct) < 5:
        confiance_globale = "Élevée"
    elif both_ok or (params["mape_ok"] and vol_ok):
        confiance_globale = "Moyenne"
    else:
        confiance_globale = "Faible"

    logging.info(
        f"   📐 {symbol} | σ_rel={volatilite_rel*100:.2f}%/j | "
        f"flat={'OUI' if not flat_ok else 'NON'} | confiance={confiance_globale}"
    )

    return {
        "dates"              : future_dates,
        "predictions"        : [float(p) for p in predictions],
        "lower_bound"        : lower_bounds,
        "upper_bound"        : upper_bounds,
        "confidence_per_day" : confidence_par_jour,
        "last_price"         : last_price,
        "last_date"          : last_date,
        "avg_change_percent" : float(variation_pct),
        "overall_confidence" : confiance_globale,
        "model_type"         : params["best_model"],
        "mape_test"          : params["mape_test"],
        "volatilite_rel_pct" : round(volatilite_rel * 100, 4),
        "pred_flat"          : not flat_ok,
    }


# ==============================================================================
# SAUVEGARDE EN BASE DE DONNÉES
# ==============================================================================

def save_predictions_to_db(conn, company_id: int, symbol: str, data: dict) -> bool:
    """
    Historisation complète — AUCUN DELETE, AUCUNE mise à jour.
    Chaque run quotidien crée NB_JOURS_PREDICTION nouvelles lignes par société.
    run_date identifie le lot. ON CONFLICT DO NOTHING protège la double exécution.
    """
    run_timestamp = datetime.now()
    run_date      = run_timestamp.date()

    try:
        with conn.cursor() as cur:
            inserted = 0
            skipped  = 0
            for i, pred_date in enumerate(data["dates"]):
                cur.execute(
                    """
                    INSERT INTO predictions (
                        company_id, prediction_date, predicted_price,
                        lower_bound, upper_bound, confidence_level,
                        created_at, run_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, prediction_date, run_date) DO NOTHING;
                    """,
                    (
                        company_id,
                        pred_date,
                        round(data["predictions"][i], 2),
                        round(data["lower_bound"][i], 2),
                        round(data["upper_bound"][i], 2),
                        data["confidence_per_day"][i],
                        run_timestamp,
                        run_date,
                    ),
                )
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1

        conn.commit()
        if skipped > 0:
            logging.info(f"✅ {symbol} : {inserted} insérée(s), {skipped} déjà présente(s) (run_date={run_date})")
        else:
            logging.info(f"✅ {symbol} : {inserted} prédictions sauvegardées (run_date={run_date})")
        return True

    except Exception as e:
        logging.error(f"❌ {symbol} : erreur sauvegarde — {e}")
        conn.rollback()
        return False


# ==============================================================================
# TRAITEMENT PAR SOCIÉTÉ
# ==============================================================================

def process_company_prediction(conn, company_id: int, symbol: str) -> bool:
    logging.info(f"--- {symbol} ---")

    if symbol not in MODELS_PARAMS:
        logging.warning(f"⚠️  {symbol} : absent de MODELS_PARAMS — ignoré")
        return False

    look_back = MODELS_PARAMS[symbol]["look_back"]

    try:
        df = pd.read_sql(
            """
            SELECT trade_date, price
            FROM historical_data
            WHERE company_id = %s
              AND price IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT %s;
            """,
            conn,
            params=(company_id, HISTORIQUE_JOURS),
        )

        logging.info(f"📊 {symbol} : {len(df)} jours disponibles")

        if len(df) < look_back:
            logging.warning(f"⚠️  {symbol} : IGNORÉ — {len(df)} jours < look_back={look_back}")
            return False

        df     = df.iloc[::-1].reset_index(drop=True)
        result = predire_10_jours(df["price"], df["trade_date"], symbol)
        if result is None:
            return False

        logging.info(
            f"📈 {symbol} | {result['model_type']} | "
            f"MAPE={result['mape_test']}% | Confiance: {result['overall_confidence']}"
        )
        logging.info(f"💰 Dernier cours : {result['last_price']:.2f} FCFA")
        for i, (d, p, c) in enumerate(
            zip(result["dates"], result["predictions"], result["confidence_per_day"])
        ):
            logging.info(f"   J+{i+1:2d} | {d.strftime('%d/%m/%Y')} | {p:10.2f} FCFA | {c}")
        logging.info(f"📊 Variation J+{NB_JOURS_PREDICTION} : {result['avg_change_percent']:+.2f}%")

        return save_predictions_to_db(conn, company_id, symbol, result)

    except Exception as e:
        logging.error(f"❌ {symbol} : erreur inattendue — {e}", exc_info=True)
        return False


# ==============================================================================
# POINT D'ENTRÉE PRINCIPAL
# ==============================================================================

def run_prediction_analysis():
    logging.info("=" * 70)
    logging.info("🔮 PREDICTIONS V17.0 — BRVM 47 ACTIONS")
    logging.info("=" * 70)
    logging.info(f"📁 Modèles      : {MODELS_DIR}")
    logging.info(f"📊 Historique   : {HISTORIQUE_JOURS} jours par action")
    logging.info(f"📈 Prédictions  : {NB_JOURS_PREDICTION} jours ouvrables")
    logging.info(f"📐 Limite BRVM  : ±{BRVM_DAILY_LIMIT*100:.1f}%/jour (borne journalière)")
    logging.info(f"🗓️  Jours fériés : {len(JOURS_FERIES)} dates exclues (2026)")
    logging.info("=" * 70)

    conn = connect_to_db()
    if not conn:
        logging.critical("❌ Connexion DB impossible — arrêt")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies ORDER BY symbol;")
            companies = cur.fetchall()

        logging.info(f"\n📋 {len(companies)} société(s) à traiter\n")

        ok, no_keras, no_scaler = [], [], []
        for _, sym in companies:
            kp, sp = _resolve_paths(sym)
            if kp and sp:
                ok.append(sym)
            elif not kp:
                no_keras.append(sym)
            else:
                no_scaler.append(sym)

        logging.info(f"✅ Prêts (keras + scaler) : {len(ok)}")
        if no_keras:
            logging.warning(f"⚠️  Sans modèle .keras   : {', '.join(no_keras)}")
        if no_scaler:
            logging.warning(f"⚠️  Sans scaler .pkl     : {', '.join(no_scaler)}")
        logging.info("")

        success, ignored = 0, 0
        for cid, sym in companies:
            if process_company_prediction(conn, cid, sym):
                success += 1
            else:
                ignored += 1

        logging.info("\n" + "=" * 70)
        logging.info("✅ PRÉDICTIONS TERMINÉES")
        logging.info(f"   📈 Succès    : {success}/{len(companies)}")
        logging.info(f"   ⚠️  Ignorés   : {ignored}")
        logging.info(f"   💾 Lignes DB : {success * NB_JOURS_PREDICTION}")
        logging.info("=" * 70)

    except Exception as e:
        logging.error(f"❌ Erreur générale : {e}", exc_info=True)
    finally:
        conn.close()


class PredictionAnalyzer:
    """Wrapper pour compatibilité avec main.py"""
    def run(self):
        run_prediction_analysis()


if __name__ == "__main__":
    run_prediction_analysis()
