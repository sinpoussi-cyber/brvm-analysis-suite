# ==============================================================================
# MODULE: PREDICTION ANALYZER V14.1 — BRVM 47 ACTIONS (Format .keras)
# ------------------------------------------------------------------------------
# VERSION: V14.1 (2026-03-07)
# CORRECTIONS:
# - Ajout du safe_mode=True pour la compatibilité des modèles
# - Gestion améliorée des erreurs de chargement
# - Fallback avec chargement manuel des poids si nécessaire
# - Support amélioré des différentes versions de Keras/TensorFlow
# ==============================================================================

import psycopg2
import pandas as pd
import numpy as np
import os
import logging
import joblib
from datetime import date, datetime, timedelta
import tensorflow as tf
from tensorflow.keras.models import load_model
from tensorflow.keras.optimizers import Adam

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')

# --- Secrets base de donnees (variables d environnement) ---
DB_NAME     = os.environ.get('DB_NAME')
DB_USER     = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST     = os.environ.get('DB_HOST')
DB_PORT     = os.environ.get('DB_PORT')

# Dossier racine des modeles :
#   MODELS_DIR/ABJC/model_ABJC.keras + scaler.pkl
MODELS_DIR = os.environ.get('MODELS_DIR', './modeles')

# Nombre fixe de jours historiques recuperes pour TOUTES les actions
HISTORIQUE_JOURS = 100

# Nombre de jours ouvrables a predire
NB_JOURS_PREDICTION = 10

# Cache memoire
_models_cache = {}


# ==============================================================================
# CALENDRIER BRVM — JOURS FERIES 2026 (Cote d Ivoire)
# ==============================================================================
JOURS_FERIES = {
    date(2026,  1,  1),  # Jour de l'An
    date(2026,  3, 17),  # Lendemain de la nuit du destin
    date(2026,  3, 20),  # Aid al-Fitr
    date(2026,  4,  6),  # Lundi de Paques
    date(2026,  5,  1),  # Fete du Travail
    date(2026,  5, 14),  # Ascension
    date(2026,  5, 27),  # Fete de la Tabaski
    date(2026,  6, 25),  # Lundi de Pentecote
    date(2026,  8,  7),  # Fete Nationale
    date(2026,  8, 15),  # Assomption
    date(2026,  8, 26),  # Lendemain de la naissance du Prophete Mahomet
    date(2026, 11,  1),  # Fete de la Toussaint
    date(2026, 11, 15),  # Journee de la Paix
    date(2026, 12, 25),  # Fete de Noel
}


def est_jour_ouvrable(d):
    """Retourne True si le jour est ouvrable pour la BRVM"""
    if isinstance(d, datetime):
        d = d.date()
    return d.weekday() <= 4 and d not in JOURS_FERIES


def prochains_jours_ouvrables(last_date, num_days=10):
    """Genere les num_days prochains jours ouvrables apres last_date"""
    if isinstance(last_date, datetime):
        last_date = last_date.date()

    result  = []
    current = last_date + timedelta(days=1)

    while len(result) < num_days:
        if est_jour_ouvrable(current):
            result.append(current)
        current += timedelta(days=1)

    return result


# ==============================================================================
# PARAMETRES DES 47 MODELES (identiques à la version précédente)
# ==============================================================================
MODELS_PARAMS = {
    "ABJC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.3029, "r2_test": 0.9702,
        "mae_test": 56.7898, "rmse_test": 82.8963,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "BICB": {
        "best_model": "GRU", "look_back": 40, "log_transform": False,
        "units": 128, "dropout": 0.3, "lr": 0.001,
        "mape_test": 0.5486, "r2_test": 0.026,
        "mae_test": 27.2097, "rmse_test": 31.9756,
        "mape_ok": True, "r2_ok": False, "source": "advanced",
    },
    "BICC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.228, "r2_test": 0.9604,
        "mae_test": 223.7151, "rmse_test": 311.1981,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "BNBC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 3.6073, "r2_test": 0.8689,
        "mae_test": 58.0034, "rmse_test": 76.1172,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "BOAB": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.2835, "r2_test": 0.9786,
        "mae_test": 72.6666, "rmse_test": 108.6415,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "BOABF": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.4895, "r2_test": 0.9334,
        "mae_test": 59.4871, "rmse_test": 85.423,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "BOAC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.2125, "r2_test": 0.8286,
        "mae_test": 86.9316, "rmse_test": 116.6847,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "BOAM": {
        "best_model": "GRU", "look_back": 60, "log_transform": False,
        "units": 128, "dropout": 0.3, "lr": 0.001,
        "mape_test": 1.2732, "r2_test": 0.919,
        "mae_test": 50.7325, "rmse_test": 75.7902,
        "mape_ok": True, "r2_ok": True, "source": "advanced",
    },
    "BOAN": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 0.9249, "r2_test": 0.4895,
        "mae_test": 23.9206, "rmse_test": 37.3064,
        "mape_ok": True, "r2_ok": False, "source": "base",
    },
    "BOAS": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.2579, "r2_test": 0.9136,
        "mae_test": 68.7593, "rmse_test": 93.3971,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "CABC": {
        "best_model": "LSTM", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 3.297, "r2_test": 0.9698,
        "mae_test": 74.8147, "rmse_test": 112.3374,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "CBIBF": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 0.8918, "r2_test": 0.9489,
        "mae_test": 97.5603, "rmse_test": 182.6674,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "CFAC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 4.9873, "r2_test": 0.9171,
        "mae_test": 74.9188, "rmse_test": 104.0824,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "CIEC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.2747, "r2_test": 0.9283,
        "mae_test": 31.3877, "rmse_test": 41.8254,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "ECOC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.6051, "r2_test": 0.9663,
        "mae_test": 242.3242, "rmse_test": 322.4865,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "ETIT": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.8994, "r2_test": 0.9016,
        "mae_test": 0.6444, "rmse_test": 0.8767,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "FTSC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 3.3064, "r2_test": 0.97,
        "mae_test": 85.1475, "rmse_test": 172.66,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "LNBB": {
        "best_model": "BiGRU", "look_back": 60, "log_transform": False,
        "units": 64, "dropout": 0.3, "lr": 0.001,
        "mape_test": 0.8472, "r2_test": 0.808,
        "mae_test": 34.0482, "rmse_test": 44.0087,
        "mape_ok": True, "r2_ok": True, "source": "advanced",
    },
    "NEIC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 3.304, "r2_test": 0.9577,
        "mae_test": 32.305, "rmse_test": 45.6816,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "NSBC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.1638, "r2_test": 0.9502,
        "mae_test": 134.0294, "rmse_test": 198.8487,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "NTLC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.2438, "r2_test": 0.7337,
        "mae_test": 263.3087, "rmse_test": 505.3043,
        "mape_ok": True, "r2_ok": False, "source": "base",
    },
    "ONTBF": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.3025, "r2_test": 0.8338,
        "mae_test": 32.1992, "rmse_test": 46.0577,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "ORAC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.1112, "r2_test": 0.6873,
        "mae_test": 163.1519, "rmse_test": 217.7093,
        "mape_ok": True, "r2_ok": False, "source": "base",
    },
    "ORGT": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 3.0779, "r2_test": 0.9122,
        "mae_test": 72.8122, "rmse_test": 97.3733,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "PALC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.7197, "r2_test": 0.8472,
        "mae_test": 143.8505, "rmse_test": 226.2105,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "PRSC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.5991, "r2_test": 0.9415,
        "mae_test": 92.9484, "rmse_test": 150.4904,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SAFC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 6.389, "r2_test": 0.9407,
        "mae_test": 188.2064, "rmse_test": 229.7859,
        "mape_ok": False, "r2_ok": True, "source": "base",
    },
    "SCRC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.2593, "r2_test": 0.908,
        "mae_test": 27.2752, "rmse_test": 38.1161,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SDCC": {
        "best_model": "BiGRU", "look_back": 60, "log_transform": False,
        "units": 128, "dropout": 0.2, "lr": 0.001,
        "mape_test": 0.894, "r2_test": 0.7271,
        "mae_test": 54.2668, "rmse_test": 83.0719,
        "mape_ok": True, "r2_ok": False, "source": "advanced",
    },
    "SDSC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.6092, "r2_test": 0.8197,
        "mae_test": 24.6265, "rmse_test": 35.4773,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SEMC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 3.0298, "r2_test": 0.9278,
        "mae_test": 48.9581, "rmse_test": 122.1576,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SGBC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.4225, "r2_test": 0.864,
        "mae_test": 401.0225, "rmse_test": 545.7458,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SHEC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.5238, "r2_test": 0.9357,
        "mae_test": 35.9683, "rmse_test": 48.6736,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SIBC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.3876, "r2_test": 0.9345,
        "mae_test": 80.5968, "rmse_test": 109.4666,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SICC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.6798, "r2_test": 0.6157,
        "mae_test": 96.6064, "rmse_test": 136.6697,
        "mape_ok": True, "r2_ok": False, "source": "base",
    },
    "SIVC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 5.8402, "r2_test": 0.9471,
        "mae_test": 82.4994, "rmse_test": 137.8727,
        "mape_ok": False, "r2_ok": True, "source": "base",
    },
    "SLBC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 2.1569, "r2_test": 0.9713,
        "mae_test": 516.3673, "rmse_test": 803.0428,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SMBC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.6586, "r2_test": 0.8521,
        "mae_test": 166.7241, "rmse_test": 233.2252,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SNTS": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 0.8219, "r2_test": 0.7655,
        "mae_test": 213.7565, "rmse_test": 317.6393,
        "mape_ok": True, "r2_ok": False, "source": "base",
    },
    "SOGC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.3089, "r2_test": 0.8368,
        "mae_test": 104.4954, "rmse_test": 159.1333,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "SPHC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.5344, "r2_test": 0.8196,
        "mae_test": 117.3221, "rmse_test": 175.6748,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "STAC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 3.9611, "r2_test": 0.9398,
        "mae_test": 45.6627, "rmse_test": 58.9501,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
    "STBC": {
        "best_model": "BiGRU", "look_back": 60, "log_transform": False,
        "units": 128, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.4802, "r2_test": 0.6015,
        "mae_test": 290.9748, "rmse_test": 470.6074,
        "mape_ok": True, "r2_ok": False, "source": "advanced",
    },
    "TTLC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 1.0205, "r2_test": 0.6746,
        "mae_test": 24.4032, "rmse_test": 38.4843,
        "mape_ok": True, "r2_ok": False, "source": "base",
    },
    "TTLS": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 0.9356, "r2_test": 0.7746,
        "mae_test": 23.7037, "rmse_test": 33.6514,
        "mape_ok": True, "r2_ok": False, "source": "base",
    },
    "UNLC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 5.7498, "r2_test": 0.9566,
        "mae_test": 2139.2477, "rmse_test": 2978.6427,
        "mape_ok": False, "r2_ok": True, "source": "base",
    },
    "UNXC": {
        "best_model": "GRU", "look_back": 20, "log_transform": False,
        "units": 64, "dropout": 0.2, "lr": 0.001,
        "mape_test": 4.6783, "r2_test": 0.9245,
        "mae_test": 65.961, "rmse_test": 92.9702,
        "mape_ok": True, "r2_ok": True, "source": "base",
    },
}


# ==============================================================================
# CONNEXION BASE DE DONNEES
# ==============================================================================

def connect_to_db():
    """Connexion à la base PostgreSQL"""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        logging.info("✅ Connexion PostgreSQL réussie")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur connexion DB: {e}")
        return None


# ==============================================================================
# CHARGEMENT DES MODELES AU FORMAT .KERAS - VERSION CORRIGÉE
# ==============================================================================

def load_action_model(symbol):
    """
    Charge le modèle Keras au format .keras en utilisant une approche robuste
    avec plusieurs tentatives de chargement pour gérer les problèmes de compatibilité.
    """
    if symbol in _models_cache:
        return _models_cache[symbol]

    action_dir = os.path.join(MODELS_DIR, symbol)

    # Vérification de l'existence du dossier
    if not os.path.isdir(action_dir):
        logging.warning(f"⚠️ {symbol} : dossier absent ({action_dir})")
        return None, None

    # Recherche du fichier .keras
    keras_files = [f for f in os.listdir(action_dir) if f.endswith(".keras")]
    if not keras_files:
        logging.warning(f"⚠️ {symbol} : aucun fichier .keras trouvé dans {action_dir}")
        return None, None

    # Vérification du scaler
    scaler_path = os.path.join(action_dir, "scaler.pkl")
    if not os.path.exists(scaler_path):
        logging.warning(f"⚠️ {symbol} : scaler.pkl absent")
        return None, None

    model_path = os.path.join(action_dir, keras_files[0])
    logging.info(f"📥 {symbol} : Chargement modèle {keras_files[0]}")

    # TENTATIVE 1: Chargement avec safe_mode=True (recommandé pour compatibilité)
    try:
        model = load_model(model_path, compile=False, safe_mode=True)
        logging.info(f"   ✓ {symbol} : Chargé avec safe_mode=True")
    except Exception as e:
        logging.warning(f"   ⚠️ {symbol} : Échec safe_mode=True - {str(e)[:100]}")
        model = None

    # TENTATIVE 2: Chargement standard sans safe_mode
    if model is None:
        try:
            model = load_model(model_path, compile=False)
            logging.info(f"   ✓ {symbol} : Chargé avec mode standard")
        except Exception as e:
            logging.warning(f"   ⚠️ {symbol} : Échec mode standard - {str(e)[:100]}")
            model = None

    # TENTATIVE 3: Chargement avec custom_objects vide
    if model is None:
        try:
            model = load_model(model_path, compile=False, custom_objects={})
            logging.info(f"   ✓ {symbol} : Chargé avec custom_objects={{}}")
        except Exception as e:
            logging.warning(f"   ⚠️ {symbol} : Échec custom_objects - {str(e)[:100]}")
            model = None

    # TENTATIVE 4: Chargement manuel des poids (dernier recours)
    if model is None:
        try:
            # Charger l'architecture et les poids séparément
            import json
            import h5py
            
            with h5py.File(model_path, 'r') as f:
                # Essayer de charger la configuration
                if 'model_config' in f.attrs:
                    config = json.loads(f.attrs['model_config'])
                    
                    # Reconstruire le modèle à partir de la config
                    from tensorflow.keras.models import model_from_json
                    model = model_from_json(json.dumps(config))
                    
                    # Charger les poids
                    model.load_weights(model_path)
                    
                    logging.info(f"   ✓ {symbol} : Chargé via reconstruction manuelle")
        except Exception as e:
            logging.error(f"   ❌ {symbol} : Échec reconstruction manuelle - {e}")
            model = None

    # Si toutes les tentatives ont échoué
    if model is None:
        logging.error(f"❌ {symbol} : Impossible de charger le modèle après 4 tentatives")
        return None, None

    # Compilation du modèle
    try:
        model.compile(optimizer=Adam(1e-3), loss="mean_squared_error")
    except Exception as e:
        logging.warning(f"   ⚠️ {symbol} : Problème compilation - {e}")
        # On continue même sans compilation réussie

    # Chargement du scaler
    try:
        scaler = joblib.load(scaler_path)
    except Exception as e:
        logging.error(f"❌ {symbol} : Erreur chargement scaler - {e}")
        return None, None

    # Affichage des paramètres si disponibles
    p = MODELS_PARAMS.get(symbol, {})
    if p:
        logging.info(
            f"   ✓ {symbol} | {p.get('best_model', 'N/A')} "
            f"look_back={p.get('look_back', 'N/A')} "
            f"MAPE={p.get('mape_test', 'N/A')}%"
        )
    else:
        logging.info(f"   ✓ {symbol} | Modèle chargé")

    _models_cache[symbol] = (model, scaler)
    return model, scaler


# ==============================================================================
# PRÉDICTION DES 10 PROCHAINS JOURS OUVRABLES
# ==============================================================================

def predire_10_jours(prices, dates, symbol):
    """
    Applique le modèle pré-entraîné pour prédire les prochains jours ouvrables
    """
    params = MODELS_PARAMS.get(symbol, {})
    if not params:
        logging.error(f"❌ {symbol} : paramètres non trouvés")
        return None
        
    model, scaler = load_action_model(symbol)

    if model is None:
        logging.error(
            f"❌ {symbol} : modèle .keras introuvable dans {MODELS_DIR}/{symbol}/"
        )
        return None

    look_back = params.get("look_back", 20)
    log_transform = params.get("log_transform", False)
    arr = np.array(prices.values, dtype=float)

    if len(arr) < look_back:
        logging.error(
            f"❌ {symbol} : {len(arr)} cours < look_back={look_back}"
        )
        return None

    last_price = float(arr[-1])
    raw_date = dates.iloc[-1]
    last_date = raw_date.date() if isinstance(raw_date, datetime) else raw_date

    future_dates = prochains_jours_ouvrables(last_date, NB_JOURS_PREDICTION)

    # Préparation de la séquence
    sequence = arr[-look_back:].copy()

    if log_transform:
        sequence = np.log1p(sequence)

    try:
        seq_scaled = scaler.transform(sequence.reshape(-1, 1))
    except Exception as e:
        logging.error(f"❌ {symbol} : Erreur transformation scaler - {e}")
        return None

    # Prédiction itérative
    current_seq = seq_scaled.copy()
    preds_scaled = []

    for _ in range(NB_JOURS_PREDICTION):
        x_input = current_seq[-look_back:].reshape(1, look_back, 1)
        try:
            p = model.predict(x_input, verbose=0)[0, 0]
        except Exception as e:
            logging.error(f"❌ {symbol} : Erreur prédiction - {e}")
            return None
        preds_scaled.append(p)
        current_seq = np.append(current_seq, [[p]], axis=0)

    # Dénormalisation
    try:
        pred_raw = scaler.inverse_transform(
            np.array(preds_scaled).reshape(-1, 1)
        ).flatten()
    except Exception as e:
        logging.error(f"❌ {symbol} : Erreur inverse transform - {e}")
        return None

    if log_transform:
        predictions = np.expm1(pred_raw)
    else:
        predictions = pred_raw

    # Intervalles de confiance
    n_recent = min(30, len(arr) - 1)
    volatilite = float(np.std(np.diff(arr[-n_recent - 1:]))) if n_recent > 0 \
                 else float(np.std(arr))

    lower_bounds = []
    upper_bounds = []
    for i, pred in enumerate(predictions):
        marge = volatilite * (1 + i * 0.05)
        lower_bounds.append(float(pred - marge))
        upper_bounds.append(float(pred + marge))

    # Niveau de confiance par jour
    mape = params.get("mape_test", 5.0)
    both_ok = params.get("mape_ok", False) and params.get("r2_ok", False)

    def _niveau_confiance(jour_idx):
        j = jour_idx + 1
        if both_ok:
            return "Élevée" if j <= 3 else "Moyenne"
        elif params.get("mape_ok", False):
            return "Moyenne" if j <= 3 else "Faible"
        else:
            return "Faible"

    confidence_par_jour = [_niveau_confiance(i) for i in range(NB_JOURS_PREDICTION)]

    variation_pct = ((float(predictions[-1]) - last_price) / last_price) * 100

    confiance_globale = (
        "Élevée"  if both_ok and abs(variation_pct) < 5 else
        "Moyenne" if params.get("mape_ok", False) else
        "Faible"
    )

    return {
        "dates": future_dates,
        "predictions": [float(p) for p in predictions],
        "lower_bound": lower_bounds,
        "upper_bound": upper_bounds,
        "confidence_per_day": confidence_par_jour,
        "last_price": last_price,
        "last_date": last_date,
        "avg_change_percent": float(variation_pct),
        "overall_confidence": confiance_globale,
        "model_type": params.get("best_model", "N/A"),
        "mape_test": mape,
    }


# ==============================================================================
# SAUVEGARDE EN BASE DE DONNEES
# ==============================================================================

def save_predictions_to_db(conn, company_id, symbol, prediction_data):
    """Sauvegarde les prédictions en base"""
    try:
        with conn.cursor() as cur:
            # Suppression des anciennes prédictions
            cur.execute("DELETE FROM predictions WHERE company_id = %s", (company_id,))

            # Insertion des nouvelles
            for i, pred_date in enumerate(prediction_data["dates"]):
                cur.execute("""
                    INSERT INTO predictions (
                        company_id, prediction_date, predicted_price,
                        lower_bound, upper_bound, confidence_level, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    company_id,
                    pred_date,
                    round(prediction_data["predictions"][i], 2),
                    round(prediction_data["lower_bound"][i], 2),
                    round(prediction_data["upper_bound"][i], 2),
                    prediction_data["confidence_per_day"][i],
                ))

            conn.commit()

        logging.info(f"✅ {symbol} : {len(prediction_data['dates'])} prédictions sauvegardées")
        return True

    except Exception as e:
        logging.error(f"❌ {symbol} : erreur sauvegarde — {e}")
        conn.rollback()
        return False


# ==============================================================================
# TRAITEMENT PAR SOCIETE
# ==============================================================================

def process_company_prediction(conn, company_id, symbol):
    """Traite une société pour générer les prédictions"""
    logging.info(f"--- {symbol} ---")

    if symbol not in MODELS_PARAMS:
        logging.warning(f"⚠️ {symbol} : absent de MODELS_PARAMS")
        return False

    look_back = MODELS_PARAMS[symbol]["look_back"]

    try:
        # Récupération des données historiques
        df = pd.read_sql(
            """
            SELECT trade_date, price
            FROM historical_data
            WHERE company_id = %s
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            conn,
            params=(company_id, HISTORIQUE_JOURS)
        )

        nb_dispo = len(df)
        logging.info(f"📊 {symbol} : {nb_dispo} jours disponibles")

        if nb_dispo < look_back:
            logging.warning(
                f"⚠️ {symbol} : IGNORÉ - {nb_dispo} jours < look_back={look_back}"
            )
            return False

        df = df.iloc[::-1].reset_index(drop=True)
        result = predire_10_jours(df["price"], df["trade_date"], symbol)

        if result is None:
            return False

        # Affichage des résultats
        logging.info(
            f"📈 {symbol} | {result['model_type']} | "
            f"MAPE={result['mape_test']}% | Confiance: {result['overall_confidence']}"
        )
        logging.info(f"💰 Dernier cours : {result['last_price']:.2f} FCFA")

        for i, (d, p, c) in enumerate(zip(
                result["dates"],
                result["predictions"],
                result["confidence_per_day"])):
            logging.info(
                f"   J+{i+1:2d} | {d.strftime('%d/%m/%Y')} | "
                f"{p:10.2f} FCFA | {c}"
            )

        logging.info(
            f"📊 Variation J+10 : {result['avg_change_percent']:+.2f}%"
        )

        return save_predictions_to_db(conn, company_id, symbol, result)

    except Exception as e:
        logging.error(f"❌ {symbol} : erreur — {e}")
        return False


# ==============================================================================
# POINT D'ENTRÉE PRINCIPAL
# ==============================================================================

def run_prediction_analysis():
    """Fonction principale d'exécution"""
    
    logging.info("=" * 70)
    logging.info("🔮 PREDICTIONS V14.1 — BRVM 47 ACTIONS (Format .keras)")
    logging.info("=" * 70)
    logging.info(f"📊 Historique : {HISTORIQUE_JOURS} jours par action")
    logging.info(f"📈 Prédictions : {NB_JOURS_PREDICTION} jours ouvrables")
    logging.info(f"📁 Modèles : {MODELS_DIR} (format .keras)")
    logging.info("=" * 70)

    # Affichage des jours fériés
    logging.info("📅 Jours fériés 2026 exclus :")
    for jf in sorted(JOURS_FERIES)[:5]:  # Afficher seulement les 5 premiers
        logging.info(f"   {jf.strftime('%d/%m/%Y')}")
    logging.info("   ...")

    conn = connect_to_db()
    if not conn:
        return

    try:
        # Récupération des sociétés
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies ORDER BY symbol;")
            companies = cur.fetchall()

        # Statistiques
        symbols = [s for _, s in companies]
        with_keras = []
        without_keras = []
        
        for sym in symbols:
            action_dir = os.path.join(MODELS_DIR, sym)
            if os.path.isdir(action_dir) and any(f.endswith('.keras') for f in os.listdir(action_dir)):
                with_keras.append(sym)
            else:
                without_keras.append(sym)

        logging.info(f"\n📊 {len(companies)} sociétés")
        logging.info(f"   ✅ Modèles .keras : {len(with_keras)}")
        logging.info(f"   ⚠️  Sans modèle : {len(without_keras)}")

        if without_keras:
            logging.warning(f"   ❌ Modèles manquants : {', '.join(without_keras[:5])}...")

        # Traitement
        success = 0
        ignored = 0
        
        for cid, sym in companies:
            if process_company_prediction(conn, cid, sym):
                success += 1
            else:
                ignored += 1

        # Résumé final
        logging.info("\n" + "=" * 70)
        logging.info("✅ PRÉDICTIONS TERMINÉES")
        logging.info(f"   📈 Succès : {success}/{len(companies)}")
        logging.info(f"   ⚠️  Ignorés : {ignored}")
        logging.info(f"   💾 Total insertions : {success * NB_JOURS_PREDICTION}")
        logging.info("=" * 70)

    except Exception as e:
        logging.error(f"❌ Erreur générale : {e}", exc_info=True)
    finally:
        conn.close()


if __name__ == "__main__":
    run_prediction_analysis()
