# ==============================================================================
# MODULE: PREDICTION ANALYZER V12.1 FINAL — BRVM 47 ACTIONS
# ------------------------------------------------------------------------------
# VERSION: V12.1 Final (2026-03-04)
# CORRECTIONS:
# - Colonne SQL: price → close_price as price (ligne 744)
# - Compatible TensorFlow 2.12.0 (requirements.txt V33.0)
# - Patch batch_shape → batch_input_shape (ligne 515-525)
# ------------------------------------------------------------------------------
# - Historique : 100 derniers jours pour toutes les actions
# - Prediction : 10 prochains jours OUVRABLES (lun-ven, hors feries CI)
# - Modeles    : GRU / LSTM / BiGRU pre-entraines, application directe (predict)
# - Aucun reentrainement : model.fit() absent du fichier
# - Source données : 100% Supabase (historical_data table)
# - Pas de fichiers .npy utilisés
# ==============================================================================

import psycopg2
import pandas as pd
import numpy as np
import os
import logging
import joblib
from datetime import date, datetime, timedelta
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
#   MODELS_DIR/ABJC/model_GRU_advanced.keras + scaler_advanced.pkl
#   MODELS_DIR/BICB/model_GRU_advanced.keras + scaler_advanced.pkl  ...
MODELS_DIR = os.environ.get('MODELS_DIR', './modeles')

# Nombre fixe de jours historiques recuperes pour TOUTES les actions
HISTORIQUE_JOURS = 100

# Nombre de jours ouvrables a predire
NB_JOURS_PREDICTION = 10

# Cache memoire : evite de recharger les fichiers .keras/.pkl a chaque appel
_models_cache = {}


# ==============================================================================
# CALENDRIER BRVM — JOURS FERIES 2026 (Cote d Ivoire)
# Liste exhaustive fournie — mise a jour annuelle recommandee
# ==============================================================================
JOURS_FERIES = {
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
    """
    Retourne True si le jour est ouvrable pour la BRVM :
      - Lundi a Vendredi (weekday 0 a 4)
      - Non ferie selon le calendrier CI 2026
    """
    if isinstance(d, datetime):
        d = d.date()
    return d.weekday() <= 4 and d not in JOURS_FERIES


def prochains_jours_ouvrables(last_date, num_days=10):
    """
    Genere les num_days prochains jours ouvrables apres last_date.
    Exclut : samedis, dimanches, jours feries CI.

    Parametres
    ----------
    last_date : date ou datetime — dernier jour de donnees historiques
    num_days  : int — nombre de jours ouvrables a generer (defaut 10)

    Retourne : liste de date
    """
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
# PARAMETRES DES 47 MODELES — integres directement dans le code
# Plus besoin de params_advanced.json sur le serveur.
#
# Champs :
#   best_model    : architecture (GRU, LSTM, BiGRU)
#   look_back     : fenetre d entree du modele lors de l entrainement
#                   (utilise uniquement pour la normalisation — voir note)
#   log_transform : True = log1p applique avant normalisation
#   units         : nb de neurones couche 1
#   dropout       : taux de dropout
#   lr            : learning rate Adam
#   mape_test     : erreur % sur jeu de test
#   r2_test       : R2 sur jeu de test
#   mae_test / rmse_test : erreurs absolues en FCFA
#   mape_ok / r2_ok      : criteres de qualite
#   source        : 'base' ou 'advanced'
#
# NOTE sur look_back vs HISTORIQUE_JOURS :
#   - HISTORIQUE_JOURS=100 : nb de jours recuperes depuis PostgreSQL
#   - look_back            : nb de jours passes dans la fenetre d entree du modele
#   On passe toujours les look_back DERNIERS jours des 100 au modele.
#   Avoir 100 jours au lieu de look_back garantit la stabilite de la normalisation.
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
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        logging.info("Connexion PostgreSQL reussie")
        return conn
    except Exception as e:
        logging.error(f"Erreur connexion DB: {e}")
        return None


# ==============================================================================
# CHARGEMENT DES MODELES PRE-ENTRAINES (.keras + .pkl)
# ==============================================================================

def load_action_model(symbol):
    """
    Charge le modele Keras et le scaler MinMaxScaler depuis le disque.
    Les parametres (look_back, log_transform, etc.) viennent de MODELS_PARAMS.
    Met en cache pour ne lire le disque qu une seule fois par session.

    Retourne (model, scaler) ou (None, None) si fichier introuvable.
    """
    if symbol in _models_cache:
        return _models_cache[symbol]

    action_dir = os.path.join(MODELS_DIR, symbol)

    if not os.path.isdir(action_dir):
        logging.warning(f"{symbol} : dossier absent ({action_dir})")
        return None, None

    keras_files = [f for f in os.listdir(action_dir) if f.endswith(".keras")]
    if not keras_files:
        logging.warning(f"{symbol} : aucun fichier .keras dans {action_dir}")
        return None, None

    scaler_path = os.path.join(action_dir, "scaler_advanced.pkl")
    if not os.path.exists(scaler_path):
        logging.warning(f"{symbol} : scaler_advanced.pkl absent")
        return None, None

    try:
        # ⚠️ PATCH COMPATIBILITÉ TensorFlow 2.12+ avec modèles anciens
        # Les modèles .h5 anciens utilisent 'batch_shape' qui a été remplacé par 'batch_input_shape'
        # Ce patch permet de charger les anciens modèles avec TensorFlow récent
        from tensorflow.keras.layers import InputLayer
        
        original_init = InputLayer.__init__
        
        def patched_init(self, *args, **kwargs):
            # Convertir batch_shape en batch_input_shape si présent
            if 'batch_shape' in kwargs:
                kwargs['batch_input_shape'] = kwargs.pop('batch_shape')
            return original_init(self, *args, **kwargs)
        
        # Appliquer le patch temporairement
        InputLayer.__init__ = patched_init
        
        model_path = os.path.join(action_dir, keras_files[0])
        model  = load_model(model_path, compile=False)
        
        # Restaurer la méthode originale
        InputLayer.__init__ = original_init
        
        model.compile(optimizer=Adam(1e-3), loss="mean_squared_error")
        scaler = joblib.load(scaler_path)

        p = MODELS_PARAMS[symbol]
        logging.info(
            f"{symbol} | modele charge : {p['best_model']} "
            f"look_back={p['look_back']} "
            f"MAPE={p['mape_test']}% R2={p['r2_test']}"
        )
        _models_cache[symbol] = (model, scaler)
        return model, scaler

    except Exception as e:
        logging.error(f"{symbol} : erreur chargement modele — {e}")
        return None, None


# ==============================================================================
# APPLICATION DU MODELE — PREDICTION DES 10 PROCHAINS JOURS OUVRABLES
# AUCUN REENTRAINEMENT : model.predict() uniquement, jamais model.fit()
# ==============================================================================

def predire_10_jours(prices, dates, symbol):
    """
    Applique le modele pre-entraine de l action sur les 100 derniers cours
    pour predire les NB_JOURS_PREDICTION prochains jours ouvrables BRVM.

    Processus (aucun entrainement) :
      1. Prendre les look_back derniers cours des 100 disponibles
      2. Normaliser avec scaler (identique a l echelle d entrainement)
      3. model.predict() iteratif sur NB_JOURS_PREDICTION jours
         chaque prediction devient l entree du jour suivant
      4. Denormaliser → valeurs en FCFA
      5. Calculer intervalles de confiance et niveaux

    Dates generees : jours ouvrables uniquement
      - Lundi a Vendredi
      - Hors jours feries CI 2026 (JOURS_FERIES)

    Retourne un dict ou None si echec.
    """
    params        = MODELS_PARAMS[symbol]
    model, scaler = load_action_model(symbol)

    if model is None:
        logging.error(
            f"{symbol} : fichier .keras introuvable — "
            f"verifiez que le modele est depose dans {MODELS_DIR}/{symbol}/"
        )
        return None

    look_back     = params["look_back"]
    log_transform = params["log_transform"]
    arr           = np.array(prices.values, dtype=float)

    # Securite : verifier qu on a au moins look_back jours
    # (normalement garanti par process_company_prediction)
    if len(arr) < look_back:
        logging.error(
            f"{symbol} : {len(arr)} cours recus < look_back={look_back}. "
            f"Impossible de predire."
        )
        return None

    # Dernier cours connu et sa date
    last_price    = float(arr[-1])
    raw_date      = dates.iloc[-1]
    last_date     = raw_date.date() if isinstance(raw_date, datetime) else raw_date

    # Dates des 10 prochains jours ouvrables (sam/dim + feries exclus)
    future_dates = prochains_jours_ouvrables(last_date, NB_JOURS_PREDICTION)

    # --- Etape 1 : prendre les look_back derniers cours ---
    sequence = arr[-look_back:].copy()

    # --- Etape 2 : transformation log si action volatile ---
    if log_transform:
        sequence = np.log1p(sequence)

    # --- Etape 3 : normalisation (meme echelle que l entrainement) ---
    seq_scaled = scaler.transform(sequence.reshape(-1, 1))

    # --- Etape 4 : prediction iterative ---
    # Aucun apprentissage — model.predict() lit les poids fixes du .keras
    current_seq    = seq_scaled.copy()
    preds_scaled   = []

    for _ in range(NB_JOURS_PREDICTION):
        x_input = current_seq[-look_back:].reshape(1, look_back, 1)
        p       = model.predict(x_input, verbose=0)[0, 0]
        preds_scaled.append(p)
        current_seq = np.append(current_seq, [[p]], axis=0)

    # --- Etape 5 : denormalisation ---
    pred_raw = scaler.inverse_transform(
        np.array(preds_scaled).reshape(-1, 1)
    ).flatten()

    if log_transform:
        predictions = np.expm1(pred_raw)
    else:
        predictions = pred_raw

    # --- Intervalles de confiance ---
    # Bases sur la volatilite journaliere des 30 derniers cours connus
    n_recent   = min(30, len(arr) - 1)
    volatilite = float(np.std(np.diff(arr[-n_recent - 1:]))) if n_recent > 0 \
                 else float(np.std(arr))

    lower_bounds = []
    upper_bounds = []
    for i, pred in enumerate(predictions):
        # L incertitude augmente avec l horizon (facteur +5% par jour)
        marge = volatilite * (1 + i * 0.05)
        lower_bounds.append(float(pred - marge))
        upper_bounds.append(float(pred + marge))

    # --- Niveau de confiance par jour ---
    mape    = params["mape_test"]
    r2      = params["r2_test"]
    both_ok = params["mape_ok"] and params["r2_ok"]

    def _niveau_confiance(jour_idx):
        j = jour_idx + 1  # J+1 a J+10
        if both_ok:
            # Les deux criteres satisfaits
            return "Elevee" if j <= 3 else "Moyenne"
        elif params["mape_ok"]:
            # MAPE OK mais R2 insuffisant (action peu liquide)
            return "Moyenne" if j <= 3 else "Faible"
        else:
            # MAPE >= 5% (action volatile : SAFC, SIVC, UNLC)
            return "Faible"

    confidence_par_jour = [_niveau_confiance(i) for i in range(NB_JOURS_PREDICTION)]

    # Variation totale J+10 vs dernier cours connu
    variation_pct = ((float(predictions[-1]) - last_price) / last_price) * 100

    confiance_globale = (
        "Elevee"  if both_ok and abs(variation_pct) < 5 else
        "Moyenne" if params["mape_ok"]                  else
        "Faible"
    )

    return {
        "dates"              : future_dates,           # liste de date (10 jours ouvrables)
        "predictions"        : [float(p) for p in predictions],
        "lower_bound"        : lower_bounds,
        "upper_bound"        : upper_bounds,
        "confidence_per_day" : confidence_par_jour,
        "last_price"         : last_price,
        "last_date"          : last_date,
        "avg_change_percent" : float(variation_pct),
        "overall_confidence" : confiance_globale,
        "model_type"         : params["best_model"],
        "mape_test"          : mape,
        "r2_test"            : r2,
    }


# ==============================================================================
# SAUVEGARDE EN BASE DE DONNEES
# ==============================================================================

def save_predictions_to_db(conn, company_id, symbol, prediction_data):
    """
    Supprime les anciennes predictions de l action et insere les nouvelles.
    10 lignes par action dans la table predictions.
    """
    try:
        with conn.cursor() as cur:
            # Suppression des predictions existantes pour cette action
            cur.execute(
                "DELETE FROM predictions WHERE company_id = %s", (company_id,))

            # Insertion des 10 nouvelles predictions
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

        logging.info(
            f"{symbol} : {len(prediction_data['dates'])} predictions "
            f"sauvegardees ({prediction_data['dates'][0]} → "
            f"{prediction_data['dates'][-1]})"
        )
        return True

    except Exception as e:
        logging.error(f"{symbol} : erreur sauvegarde — {e}")
        conn.rollback()
        return False


# ==============================================================================
# TRAITEMENT PAR SOCIETE
# ==============================================================================

def process_company_prediction(conn, company_id, symbol):
    """
    Pour une action :
      1. Recupere les 100 derniers cours depuis historical_data (PostgreSQL)
      2. Verifie qu on a au moins look_back jours (sinon ignore l action)
      3. Applique le modele pre-entraine → predire_10_jours()
      4. Sauvegarde en base → save_predictions_to_db()

    Pas de reentrainement, pas de fallback lineaire.
    """
    logging.info(f"--- {symbol} ---")

    if symbol not in MODELS_PARAMS:
        logging.warning(f"{symbol} : absent de MODELS_PARAMS, ignore.")
        return False

    look_back = MODELS_PARAMS[symbol]["look_back"]

    try:
        # Recuperation des 100 derniers cours (fixes pour toutes les actions)
        df = pd.read_sql(
            """
            SELECT trade_date, close_price as price
            FROM historical_data
            WHERE company_id = %s
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            conn,
            params=(company_id, HISTORIQUE_JOURS)
        )

        nb_dispo = len(df)
        logging.info(f"{symbol} : {nb_dispo} jours disponibles "
                     f"(look_back={look_back}, historique requis={HISTORIQUE_JOURS})")

        # Verification minimale : il faut au moins look_back jours pour le modele
        if nb_dispo < look_back:
            logging.warning(
                f"{symbol} : IGNORE — seulement {nb_dispo} jours disponibles "
                f"alors que le modele necessite look_back={look_back} jours minimum."
            )
            return False

        if nb_dispo < HISTORIQUE_JOURS:
            logging.warning(
                f"{symbol} : seulement {nb_dispo}/{HISTORIQUE_JOURS} jours disponibles. "
                f"Prediction quand meme possible (look_back={look_back} satisfait)."
            )

        # Remise en ordre chronologique (du plus ancien au plus recent)
        df = df.iloc[::-1].reset_index(drop=True)

        # Application du modele pre-entraine
        result = predire_10_jours(df["price"], df["trade_date"], symbol)

        if result is None:
            return False

        # Affichage du resume dans les logs
        logging.info(
            f"{symbol} | {result['model_type']} | "
            f"MAPE={result['mape_test']}% | R2={result['r2_test']} | "
            f"Confiance globale: {result['overall_confidence']}"
        )
        logging.info(
            f"{symbol} | Dernier cours : {result['last_price']:.2f} FCFA "
            f"({result['last_date']})"
        )

        # Afficher les 10 predictions
        for i, (d, p, c) in enumerate(zip(
                result["dates"],
                result["predictions"],
                result["confidence_per_day"])):
            ferie_info = " [FERIE]" if d in JOURS_FERIES else ""
            logging.info(
                f"  J+{i+1:2d} | {d} ({d.strftime('%A')}{ferie_info}) | "
                f"{p:10.2f} FCFA | {c}"
            )

        logging.info(
            f"{symbol} | Variation J+10 vs aujourd hui : "
            f"{result['avg_change_percent']:+.2f}%"
        )

        # Sauvegarde en base
        return save_predictions_to_db(conn, company_id, symbol, result)

    except Exception as e:
        logging.error(f"{symbol} : erreur inattendue — {e}", exc_info=True)
        return False


# ==============================================================================
# POINT D ENTREE PRINCIPAL
# ==============================================================================

def run_prediction_analysis():
    logging.info("=" * 70)
    logging.info("PREDICTIONS V11.0 — BRVM 47 ACTIONS")
    logging.info(f"Historique : {HISTORIQUE_JOURS} jours par action")
    logging.info(f"Predictions : {NB_JOURS_PREDICTION} jours ouvrables")
    logging.info(f"Calendrier : jours feries CI 2026 exclus ({len(JOURS_FERIES)} jours)")
    logging.info(f"Modeles : {MODELS_DIR}")
    logging.info("=" * 70)

    # Afficher le calendrier des jours feries
    logging.info("Jours feries CI 2026 exclus des predictions :")
    for jf in sorted(JOURS_FERIES):
        logging.info(f"  {jf} ({jf.strftime('%A %d %B %Y')})")

    conn = connect_to_db()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies ORDER BY symbol;")
            companies = cur.fetchall()

        symbols   = [s for _, s in companies]
        known     = [s for s in symbols if s     in MODELS_PARAMS]
        unknown   = [s for s in symbols if s not in MODELS_PARAMS]
        has_files = [s for s in known if os.path.isdir(os.path.join(MODELS_DIR, s))]
        no_files  = [s for s in known if not os.path.isdir(os.path.join(MODELS_DIR, s))]

        logging.info(f"\n{len(companies)} societes | "
                     f"params: {len(known)}/47 | "
                     f"fichiers .keras: {len(has_files)}/{len(known)}")

        if no_files:
            logging.warning(
                f"Modeles absents du serveur ({len(no_files)}) : {no_files}"
            )
        if unknown:
            logging.warning(f"Actions hors MODELS_PARAMS : {unknown}")

        # Traitement de toutes les societes
        success = 0
        ignored = 0
        for cid, sym in companies:
            ok = process_company_prediction(conn, cid, sym)
            if ok:
                success += 1
            else:
                ignored += 1

        logging.info("=" * 70)
        logging.info(f"TERMINE : {success}/{len(companies)} predictions reussies")
        logging.info(f"Ignores : {ignored} (fichier manquant ou donnees insuffisantes)")
        logging.info(f"Total lignes inserees : {success * NB_JOURS_PREDICTION}")
        logging.info("=" * 70)

    except Exception as e:
        logging.error(f"Erreur generale : {e}", exc_info=True)
    finally:
        conn.close()


if __name__ == "__main__":
    run_prediction_analysis()
