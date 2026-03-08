# ==============================================================================
# MODULE: PREDICTION ANALYZER V15.1 — BRVM 47 ACTIONS
# ------------------------------------------------------------------------------
# Structure exacte des dossiers modeles/ :
#
#   SOURCE "base"     (42 symboles) :
#       modeles/SYMBOL/model_*.keras          (sans "_advanced")
#       modeles/SYMBOL/scaler.pkl
#
#   SOURCE "advanced" (5 symboles : BICB, BOAM, LNBB, SDCC, STBC) :
#       modeles/SYMBOL/model_*_advanced.keras
#       modeles/SYMBOL/scaler_advanced.pkl
#
# Corrections V15.1 :
#   - _resolve_paths() : sélection automatique du bon .keras ET du bon .pkl
#     selon le champ "source" dans MODELS_PARAMS (pas keras_files[0] aveugle)
#   - 4 tentatives de chargement du modèle (safe_mode → standard → custom_objects → H5)
#   - Requêtes SQL entièrement paramétrées (pas d'injection possible)
#   - Intervalles de confiance dynamiques (volatilité réelle, borne inf ≥ 0)
#   - Pré-audit affiché au démarrage (OK / sans keras / sans scaler)
#   - Classe PredictionAnalyzer pour compatibilité avec main.py
# ==============================================================================

import os
import logging
import joblib
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import tensorflow as tf
from tensorflow.keras.models import load_model
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

# Dossier racine des modèles (peut être surchargé via env)
MODELS_DIR = os.environ.get("MODELS_DIR", "./modeles")

# Nombre de jours historiques récupérés en base
HISTORIQUE_JOURS = 100

# Nombre de jours ouvrables à prédire
NB_JOURS_PREDICTION = 10

# Cache mémoire : évite de recharger modèle + scaler à chaque appel
_models_cache: dict = {}


# ==============================================================================
# CALENDRIER BRVM — JOURS FÉRIÉS 2026 (Côte d'Ivoire)
# ==============================================================================
JOURS_FERIES = {
    date(2026,  1,  1),   # Jour de l'An
    date(2026,  3, 17),   # Lendemain de la nuit du destin
    date(2026,  3, 20),   # Aïd al-Fitr
    date(2026,  4,  6),   # Lundi de Pâques
    date(2026,  5,  1),   # Fête du Travail
    date(2026,  5, 14),   # Ascension
    date(2026,  5, 27),   # Fête de la Tabaski
    date(2026,  6, 25),   # Lundi de Pentecôte
    date(2026,  8,  7),   # Fête Nationale
    date(2026,  8, 15),   # Assomption
    date(2026,  8, 26),   # Lendemain de la naissance du Prophète
    date(2026, 11,  1),   # Toussaint
    date(2026, 11, 15),   # Journée de la Paix
    date(2026, 12, 25),   # Noël
}


def est_jour_ouvrable(d: date) -> bool:
    """True si le jour est ouvrable pour la BRVM (lun-ven, hors fériés)."""
    if isinstance(d, datetime):
        d = d.date()
    return d.weekday() <= 4 and d not in JOURS_FERIES


def prochains_jours_ouvrables(last_date: date, num_days: int = 10) -> list:
    """Retourne les `num_days` prochains jours ouvrables après `last_date`."""
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
               "mae_test": 56.7898,  "rmse_test": 82.8963,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "BICB":  {"best_model": "GRU",   "look_back": 40, "log_transform": False,
               "units": 128, "dropout": 0.3, "lr": 0.001,
               "mape_test": 0.5486,  "r2_test": 0.026,
               "mae_test": 27.2097,  "rmse_test": 31.9756,
               "mape_ok": True,  "r2_ok": False, "source": "advanced"},

    "BICC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.228,   "r2_test": 0.9604,
               "mae_test": 223.7151, "rmse_test": 311.1981,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "BNBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.6073,  "r2_test": 0.8689,
               "mae_test": 58.0034,  "rmse_test": 76.1172,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "BOAB":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2835,  "r2_test": 0.9786,
               "mae_test": 72.6666,  "rmse_test": 108.6415,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "BOABF": {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.4895,  "r2_test": 0.9334,
               "mae_test": 59.4871,  "rmse_test": 85.423,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "BOAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2125,  "r2_test": 0.8286,
               "mae_test": 86.9316,  "rmse_test": 116.6847,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "BOAM":  {"best_model": "GRU",   "look_back": 60, "log_transform": False,
               "units": 128, "dropout": 0.3, "lr": 0.001,
               "mape_test": 1.2732,  "r2_test": 0.919,
               "mae_test": 50.7325,  "rmse_test": 75.7902,
               "mape_ok": True,  "r2_ok": True,  "source": "advanced"},

    "BOAN":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.9249,  "r2_test": 0.4895,
               "mae_test": 23.9206,  "rmse_test": 37.3064,
               "mape_ok": True,  "r2_ok": False, "source": "base"},

    "BOAS":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2579,  "r2_test": 0.9136,
               "mae_test": 68.7593,  "rmse_test": 93.3971,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "CABC":  {"best_model": "LSTM",  "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.297,   "r2_test": 0.9698,
               "mae_test": 74.8147,  "rmse_test": 112.3374,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "CBIBF": {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.8918,  "r2_test": 0.9489,
               "mae_test": 97.5603,  "rmse_test": 182.6674,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "CFAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 4.9873,  "r2_test": 0.9171,
               "mae_test": 74.9188,  "rmse_test": 104.0824,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "CIEC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.2747,  "r2_test": 0.9283,
               "mae_test": 31.3877,  "rmse_test": 41.8254,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "ECOC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.6051,  "r2_test": 0.9663,
               "mae_test": 242.3242, "rmse_test": 322.4865,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "ETIT":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.8994,  "r2_test": 0.9016,
               "mae_test": 0.6444,   "rmse_test": 0.8767,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "FTSC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.3064,  "r2_test": 0.97,
               "mae_test": 85.1475,  "rmse_test": 172.66,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "LNBB":  {"best_model": "BiGRU", "look_back": 60, "log_transform": False,
               "units": 64,  "dropout": 0.3, "lr": 0.001,
               "mape_test": 0.8472,  "r2_test": 0.808,
               "mae_test": 34.0482,  "rmse_test": 44.0087,
               "mape_ok": True,  "r2_ok": True,  "source": "advanced"},

    "NEIC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.304,   "r2_test": 0.9577,
               "mae_test": 32.305,   "rmse_test": 45.6816,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "NSBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.1638,  "r2_test": 0.9502,
               "mae_test": 134.0294, "rmse_test": 198.8487,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "NTLC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.2438,  "r2_test": 0.7337,
               "mae_test": 263.3087, "rmse_test": 505.3043,
               "mape_ok": True,  "r2_ok": False, "source": "base"},

    "ONTBF": {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.3025,  "r2_test": 0.8338,
               "mae_test": 32.1992,  "rmse_test": 46.0577,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "ORAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.1112,  "r2_test": 0.6873,
               "mae_test": 163.1519, "rmse_test": 217.7093,
               "mape_ok": True,  "r2_ok": False, "source": "base"},

    "ORGT":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.0779,  "r2_test": 0.9122,
               "mae_test": 72.8122,  "rmse_test": 97.3733,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "PALC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.7197,  "r2_test": 0.8472,
               "mae_test": 143.8505, "rmse_test": 226.2105,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "PRSC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.5991,  "r2_test": 0.9415,
               "mae_test": 92.9484,  "rmse_test": 150.4904,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SAFC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 6.389,   "r2_test": 0.9407,
               "mae_test": 188.2064, "rmse_test": 229.7859,
               "mape_ok": False, "r2_ok": True,  "source": "base"},

    "SCRC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.2593,  "r2_test": 0.908,
               "mae_test": 27.2752,  "rmse_test": 38.1161,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SDCC":  {"best_model": "BiGRU", "look_back": 60, "log_transform": False,
               "units": 128, "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.894,   "r2_test": 0.7271,
               "mae_test": 54.2668,  "rmse_test": 83.0719,
               "mape_ok": True,  "r2_ok": False, "source": "advanced"},

    "SDSC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.6092,  "r2_test": 0.8197,
               "mae_test": 24.6265,  "rmse_test": 35.4773,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SEMC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.0298,  "r2_test": 0.9278,
               "mae_test": 48.9581,  "rmse_test": 122.1576,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SGBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.4225,  "r2_test": 0.864,
               "mae_test": 401.0225, "rmse_test": 545.7458,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SHEC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.5238,  "r2_test": 0.9357,
               "mae_test": 35.9683,  "rmse_test": 48.6736,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SIBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.3876,  "r2_test": 0.9345,
               "mae_test": 80.5968,  "rmse_test": 109.4666,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SICC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.6798,  "r2_test": 0.6157,
               "mae_test": 96.6064,  "rmse_test": 136.6697,
               "mape_ok": True,  "r2_ok": False, "source": "base"},

    "SIVC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 5.8402,  "r2_test": 0.9471,
               "mae_test": 82.4994,  "rmse_test": 137.8727,
               "mape_ok": False, "r2_ok": True,  "source": "base"},

    "SLBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 2.1569,  "r2_test": 0.9713,
               "mae_test": 516.3673, "rmse_test": 803.0428,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SMBC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.6586,  "r2_test": 0.8521,
               "mae_test": 166.7241, "rmse_test": 233.2252,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SNTS":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.8219,  "r2_test": 0.7655,
               "mae_test": 213.7565, "rmse_test": 317.6393,
               "mape_ok": True,  "r2_ok": False, "source": "base"},

    "SOGC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.3089,  "r2_test": 0.8368,
               "mae_test": 104.4954, "rmse_test": 159.1333,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "SPHC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.5344,  "r2_test": 0.8196,
               "mae_test": 117.3221, "rmse_test": 175.6748,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "STAC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 3.9611,  "r2_test": 0.9398,
               "mae_test": 45.6627,  "rmse_test": 58.9501,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},

    "STBC":  {"best_model": "BiGRU", "look_back": 60, "log_transform": False,
               "units": 128, "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.4802,  "r2_test": 0.6015,
               "mae_test": 290.9748, "rmse_test": 470.6074,
               "mape_ok": True,  "r2_ok": False, "source": "advanced"},

    "TTLC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 1.0205,  "r2_test": 0.6746,
               "mae_test": 24.4032,  "rmse_test": 38.4843,
               "mape_ok": True,  "r2_ok": False, "source": "base"},

    "TTLS":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 0.9356,  "r2_test": 0.7746,
               "mae_test": 23.7037,  "rmse_test": 33.6514,
               "mape_ok": True,  "r2_ok": False, "source": "base"},

    "UNLC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 5.7498,  "r2_test": 0.9566,
               "mae_test": 2139.2477,"rmse_test": 2978.6427,
               "mape_ok": False, "r2_ok": True,  "source": "base"},

    "UNXC":  {"best_model": "GRU",   "look_back": 20, "log_transform": False,
               "units": 64,  "dropout": 0.2, "lr": 0.001,
               "mape_test": 4.6783,  "r2_test": 0.9245,
               "mae_test": 65.961,   "rmse_test": 92.9702,
               "mape_ok": True,  "r2_ok": True,  "source": "base"},
}


# ==============================================================================
# RÉSOLUTION DES CHEMINS (keras + scaler) SELON "source"
# ==============================================================================

def _resolve_paths(symbol: str):
    """
    Retourne (keras_path, scaler_path) selon la convention de nommage réelle :

      source == "base"     → model_*.keras (sans _advanced) + scaler.pkl
      source == "advanced" → model_*_advanced.keras          + scaler_advanced.pkl

    Retourne (None, None) si un fichier est introuvable.
    """
    action_dir = os.path.join(MODELS_DIR, symbol)

    if not os.path.isdir(action_dir):
        logging.warning(f"⚠️  {symbol} : dossier absent ({action_dir})")
        return None, None

    source    = MODELS_PARAMS.get(symbol, {}).get("source", "base")
    all_files = os.listdir(action_dir)

    # ---- Sélection du fichier .keras ----
    keras_files = [f for f in all_files if f.endswith(".keras")]
    if not keras_files:
        logging.warning(f"⚠️  {symbol} : aucun fichier .keras dans {action_dir}")
        return None, None

    if source == "advanced":
        candidates = [f for f in keras_files if "_advanced" in f]
    else:
        candidates = [f for f in keras_files if "_advanced" not in f]

    # Fallback : prendre le premier .keras disponible si le candidat idéal manque
    keras_file = sorted(candidates)[0] if candidates else sorted(keras_files)[0]
    keras_path = os.path.join(action_dir, keras_file)

    # ---- Sélection du fichier scaler ----
    scaler_name = "scaler_advanced.pkl" if source == "advanced" else "scaler.pkl"
    scaler_path = os.path.join(action_dir, scaler_name)

    if not os.path.exists(scaler_path):
        logging.warning(f"⚠️  {symbol} : {scaler_name} absent dans {action_dir}")
        return None, None

    return keras_path, scaler_path


# ==============================================================================
# CONNEXION BASE DE DONNÉES
# ==============================================================================

def connect_to_db():
    """Connexion à la base PostgreSQL. Retourne la connexion ou None."""
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
# CHARGEMENT DU MODÈLE ET DU SCALER
# ==============================================================================

def load_action_model(symbol: str):
    """
    Charge (model, scaler) depuis le cache ou depuis le disque.
    Retourne (None, None) en cas d'échec.
    """
    if symbol in _models_cache:
        return _models_cache[symbol]

    keras_path, scaler_path = _resolve_paths(symbol)
    if keras_path is None:
        return None, None

    logging.info(f"📥 {symbol} : chargement de {os.path.basename(keras_path)}")

    model = None

    # Tentative 1 — safe_mode=True (compatibilité maximale inter-versions)
    try:
        model = load_model(keras_path, compile=False, safe_mode=True)
        logging.info(f"   ✓ {symbol} : safe_mode=True")
    except Exception as e:
        logging.warning(f"   ↳ safe_mode=True échoué : {str(e)[:80]}")

    # Tentative 2 — mode standard
    if model is None:
        try:
            model = load_model(keras_path, compile=False)
            logging.info(f"   ✓ {symbol} : mode standard")
        except Exception as e:
            logging.warning(f"   ↳ mode standard échoué : {str(e)[:80]}")

    # Tentative 3 — custom_objects vide
    if model is None:
        try:
            model = load_model(keras_path, compile=False, custom_objects={})
            logging.info(f"   ✓ {symbol} : custom_objects={{}}")
        except Exception as e:
            logging.warning(f"   ↳ custom_objects échoué : {str(e)[:80]}")

    # Tentative 4 — reconstruction manuelle depuis H5
    if model is None:
        try:
            import json
            import h5py
            from tensorflow.keras.models import model_from_json
            with h5py.File(keras_path, "r") as f:
                if "model_config" in f.attrs:
                    config = json.loads(f.attrs["model_config"])
                    model = model_from_json(json.dumps(config))
                    model.load_weights(keras_path)
                    logging.info(f"   ✓ {symbol} : reconstruction H5")
        except Exception as e:
            logging.error(f"   ↳ reconstruction H5 échouée : {e}")

    if model is None:
        logging.error(f"❌ {symbol} : modèle non chargeable après 4 tentatives")
        return None, None

    # Compilation (non bloquante)
    try:
        model.compile(optimizer=Adam(learning_rate=1e-3), loss="mean_squared_error")
    except Exception as e:
        logging.warning(f"   ↳ compilation échouée (non bloquant) : {e}")

    # Chargement du scaler
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
# PRÉDICTION DES NB_JOURS_PREDICTION PROCHAINS JOURS OUVRABLES
# ==============================================================================

def predire_10_jours(prices: pd.Series, dates: pd.Series, symbol: str):
    """
    Génère les prédictions pour les NB_JOURS_PREDICTION jours ouvrables suivants.

    Retourne un dict ou None en cas d'échec.
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
        logging.error(f"❌ {symbol} : {len(arr)} jours disponibles < look_back={look_back}")
        return None

    # Dernière date et cours connus
    last_price = float(arr[-1])
    raw_date   = dates.iloc[-1]
    last_date  = raw_date.date() if isinstance(raw_date, datetime) else raw_date
    future_dates = prochains_jours_ouvrables(last_date, NB_JOURS_PREDICTION)

    # Normalisation de la séquence d'entrée
    sequence = arr[-look_back:].copy()
    if log_transform:
        sequence = np.log1p(sequence)

    try:
        seq_scaled = scaler.transform(sequence.reshape(-1, 1))
    except Exception as e:
        logging.error(f"❌ {symbol} : normalisation scaler échouée — {e}")
        return None

    # Prédiction itérative (auto-régressive)
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

    # Dénormalisation
    try:
        pred_raw = scaler.inverse_transform(
            np.array(preds_scaled).reshape(-1, 1)
        ).flatten()
    except Exception as e:
        logging.error(f"❌ {symbol} : dénormalisation échouée — {e}")
        return None

    predictions = np.expm1(pred_raw) if log_transform else pred_raw

    # Intervalles de confiance (volatilité réelle, marge croissante dans le temps)
    n_recent   = min(30, len(arr) - 1)
    volatilite = (
        float(np.std(np.diff(arr[-n_recent - 1:])))
        if n_recent > 0 else float(np.std(arr))
    )

    lower_bounds, upper_bounds = [], []
    for i, pred in enumerate(predictions):
        marge = volatilite * (1 + i * 0.05)
        lower_bounds.append(float(max(0.0, pred - marge)))   # jamais négatif
        upper_bounds.append(float(pred + marge))

    # Niveaux de confiance par jour
    both_ok = params["mape_ok"] and params["r2_ok"]

    def _niveau(j_idx: int) -> str:
        j = j_idx + 1
        if both_ok:
            return "Élevée" if j <= 3 else "Moyenne"
        if params["mape_ok"]:
            return "Moyenne" if j <= 3 else "Faible"
        return "Faible"

    confidence_par_jour = [_niveau(i) for i in range(NB_JOURS_PREDICTION)]

    variation_pct = (
        ((float(predictions[-1]) - last_price) / last_price) * 100
        if last_price else 0.0
    )
    confiance_globale = (
        "Élevée"  if both_ok and abs(variation_pct) < 5 else
        "Moyenne" if params["mape_ok"] else
        "Faible"
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
    }


# ==============================================================================
# SAUVEGARDE EN BASE DE DONNÉES
# ==============================================================================

def save_predictions_to_db(conn, company_id: int, symbol: str, data: dict) -> bool:
    """
    Supprime les prédictions existantes et insère les nouvelles.
    Requêtes entièrement paramétrées.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM predictions WHERE company_id = %s;",
                (company_id,)
            )
            for i, pred_date in enumerate(data["dates"]):
                cur.execute(
                    """
                    INSERT INTO predictions (
                        company_id, prediction_date, predicted_price,
                        lower_bound, upper_bound, confidence_level, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                    """,
                    (
                        company_id,
                        pred_date,
                        round(data["predictions"][i], 2),
                        round(data["lower_bound"][i], 2),
                        round(data["upper_bound"][i], 2),
                        data["confidence_per_day"][i],
                    ),
                )
        conn.commit()
        logging.info(f"✅ {symbol} : {len(data['dates'])} prédictions sauvegardées")
        return True
    except Exception as e:
        logging.error(f"❌ {symbol} : erreur sauvegarde — {e}")
        conn.rollback()
        return False


# ==============================================================================
# TRAITEMENT PAR SOCIÉTÉ
# ==============================================================================

def process_company_prediction(conn, company_id: int, symbol: str) -> bool:
    """Récupère l'historique, génère les prédictions et les sauvegarde."""
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
            logging.warning(
                f"⚠️  {symbol} : IGNORÉ — {len(df)} jours < look_back={look_back}"
            )
            return False

        # Remettre dans l'ordre chronologique (DESC → ASC)
        df = df.iloc[::-1].reset_index(drop=True)

        result = predire_10_jours(df["price"], df["trade_date"], symbol)
        if result is None:
            return False

        # Affichage
        logging.info(
            f"📈 {symbol} | {result['model_type']} | "
            f"MAPE={result['mape_test']}% | Confiance: {result['overall_confidence']}"
        )
        logging.info(f"💰 Dernier cours : {result['last_price']:.2f} FCFA")
        for i, (d, p, c) in enumerate(
            zip(result["dates"], result["predictions"], result["confidence_per_day"])
        ):
            logging.info(
                f"   J+{i + 1:2d} | {d.strftime('%d/%m/%Y')} | {p:10.2f} FCFA | {c}"
            )
        logging.info(
            f"📊 Variation J+{NB_JOURS_PREDICTION} : {result['avg_change_percent']:+.2f}%"
        )

        return save_predictions_to_db(conn, company_id, symbol, result)

    except Exception as e:
        logging.error(f"❌ {symbol} : erreur inattendue — {e}", exc_info=True)
        return False


# ==============================================================================
# POINT D'ENTRÉE PRINCIPAL
# ==============================================================================

def run_prediction_analysis():
    """Lance l'analyse de prédiction pour toutes les sociétés en base."""

    logging.info("=" * 70)
    logging.info("🔮 PREDICTIONS V15.1 — BRVM 47 ACTIONS")
    logging.info("=" * 70)
    logging.info(f"📁 Modèles      : {MODELS_DIR}")
    logging.info(f"📊 Historique   : {HISTORIQUE_JOURS} jours par action")
    logging.info(f"📈 Prédictions  : {NB_JOURS_PREDICTION} jours ouvrables")
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

        # Pré-audit
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

        # Traitement
        success, ignored = 0, 0
        for cid, sym in companies:
            if process_company_prediction(conn, cid, sym):
                success += 1
            else:
                ignored += 1

        # Résumé
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


# Classe wrapper pour compatibilité avec main.py
class PredictionAnalyzer:
    def run(self):
        run_prediction_analysis()


if __name__ == "__main__":
    run_prediction_analysis()
