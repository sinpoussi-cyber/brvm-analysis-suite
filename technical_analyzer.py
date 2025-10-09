# ==============================================================================
# MODULE: TECHNICAL ANALYZER V7.0 - ÉCRITURE DANS COLONNES F-X
# ==============================================================================

import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import warnings
import os
import logging
import time
import json
import gspread
from google.oauth2 import service_account
from datetime import datetime

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# --- Connexion DB ---
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        logging.info("✅ Connexion PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur connexion DB: {e}")
        return None

# --- Authentification Google Sheets ---
def authenticate_gsheets():
    try:
        if not GSPREAD_SERVICE_ACCOUNT_JSON:
            return None
        
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google Sheets réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur authentification Google Sheets: {e}")
        return None

# --- Calcul Moyennes Mobiles ---
def calculate_moving_averages(df, price_col='price'):
    df['mm5'] = df[price_col].rolling(window=5).mean()
    df['mm10'] = df[price_col].rolling(window=10).mean()
    df['mm20'] = df[price_col].rolling(window=20).mean()
    df['mm50'] = df[price_col].rolling(window=50).mean()
    
    def mm_decision(row):
        price, mm5, mm10, mm20, mm50 = row[price_col], row['mm5'], row['mm10'], row['mm20'], row['mm50']
        if any(pd.isna(val) for val in [price, mm5, mm10, mm20, mm50]): 
            return "Attendre"
        if ((price > mm5) and (mm5 > mm10)) or ((mm5 > mm10) and (mm10 > mm20)) or ((mm10 > mm20) and (mm20 > mm50)): 
            return "Achat"
        return "Vente"
    
    df['mm_decision'] = df.apply(mm_decision, axis=1)
    return df

# --- Calcul Bandes de Bollinger ---
def calculate_bollinger_bands(df, price_col='price', window=35, num_std=2):
    df['bollinger_central'] = df[price_col].rolling(window=window).mean()
    rolling_std = df[price_col].rolling(window=window).std()
    df['bollinger_superior'] = df['bollinger_central'] + (rolling_std * num_std)
    df['bollinger_inferior'] = df['bollinger_central'] - (rolling_std * num_std)
    
    def bollinger_decision(row):
        price, lower, upper = row[price_col], row['bollinger_inferior'], row['bollinger_superior']
        if any(pd.isna(val) for val in [price, lower, upper]): 
            return "Attendre"
        if price <= lower: 
            return "Achat"
        if price >= upper: 
            return "Vente"
        return "Neutre"
    
    df['bollinger_decision'] = df.apply(bollinger_decision, axis=1)
    return df

# --- Calcul MACD ---
def calculate_macd(df, price_col='price', fast=12, slow=26, signal=9):
    df['macd_line'] = df[price_col].ewm(span=fast, adjust=False).mean() - df[price_col].ewm(span=slow, adjust=False).mean()
    df['signal_line'] = df['macd_line'].ewm(span=signal, adjust=False).mean()
    df['histogram'] = df['macd_line'] - df['signal_line']
    df['prev_histo'] = df['histogram'].shift(1)
    
    def macd_decision(row):
        if pd.isna(row['histogram']) or pd.isna(row['prev_histo']): 
            return "Attendre"
        if row['prev_histo'] <= 0 and row['histogram'] > 0: 
            return "Achat (Fort)"
        if row['prev_histo'] >= 0 and row['histogram'] < 0: 
            return "Vente (Fort)"
        if row['histogram'] > 0: 
            return "Achat"
        if row['histogram'] < 0: 
            return "Vente"
        return "Neutre"
    
    df['macd_decision'] = df.apply(macd_decision, axis=1)
    return df

# --- Calcul RSI ---
def calculate_rsi(df, price_col='price', period=20):
    delta = df[price_col].diff(1)
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.where(delta < 0, 0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi'] = 100 - (100 / (1 + rs))
    df['rs'] = rs
    df['prev_rsi'] = df['rsi'].shift(1)
    
    def rsi_decision(row):
        if pd.isna(row['rsi']) or pd.isna(row['prev_rsi']): 
            return "Attendre"
        if row['prev_rsi'] <= 30 and row['rsi'] > 30: 
            return "Achat"
        if row['prev_rsi'] >= 70 and row['rsi'] < 70: 
            return "Vente"
        return "Neutre"
    
    df['rsi_decision'] = df.apply(rsi_decision, axis=1)
    return df

# --- Calcul Stochastique ---
def calculate_stochastic(df, price_col='price', k_period=20, d_period=5):
    rolling_high = df[price_col].rolling(window=k_period).max()
    rolling_low = df[price_col].rolling(window=k_period).min()
    df['stochastic_k'] = 100 * ((df[price_col] - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan))
    df['stochastic_d'] = df['stochastic_k'].rolling(window=d_period).mean()
    df['prev_k'] = df['stochastic_k'].shift(1)
    df['prev_d'] = df['stochastic_d'].shift(1)
    
    def stochastic_decision(row):
        if any(pd.isna(val) for val in [row['stochastic_k'], row['stochastic_d'], row['prev_k'], row['prev_d']]): 
            return "Attendre"
        if row['prev_k'] <= row['prev_d'] and row['stochastic_k'] > row['stochastic_d'] and row['stochastic_d'] < 20: 
            return "Achat (Fort)"
        if row['prev_k'] >= row['prev_d'] and row['stochastic_k'] < row['stochastic_d'] and row['stochastic_d'] > 80: 
            return "Vente (Fort)"
        return "Neutre"
    
    df['stochastic_decision'] = df.apply(stochastic_decision, axis=1)
    return df

# --- Écriture dans Google Sheets (Colonnes F-X) ---
def write_technical_to_gsheet_columns(gc, spreadsheet, symbol, df):
    """Écrit l'analyse technique dans les colonnes F à X de la feuille existante"""
    try:
        worksheet = spreadsheet.worksheet(symbol)
        
        # Récupérer toutes les dates existantes (colonne B)
        all_values = worksheet.get_all_values()
        
        if len(all_values) <= 1:
            logging.warning(f"   ⚠️  Feuille {symbol} vide ou sans données")
            return False
        
        # Créer un mapping date -> numéro de ligne
        date_to_row = {}
        for idx, row in enumerate(all_values[1:], start=2):  # Ligne 2 = première donnée
            if len(row) >= 2:
                date_to_row[row[1]] = idx  # row[1] = colonne B (Date)
        
        # Préparer les mises à jour par lot
        updates = []
        
        for _, data_row in df.iterrows():
            date_str = data_row['trade_date'].strftime('%d/%m/%Y')
            
            if date_str not in date_to_row:
                continue
            
            row_num = date_to_row[date_str]
            
            # Préparer les valeurs pour colonnes F à X
            values = [
                data_row.get('mm5', ''),
                data_row.get('mm10', ''),
                data_row.get('mm20', ''),
                data_row.get('mm50', ''),
                data_row.get('mm_decision', ''),
                data_row.get('bollinger_central', ''),
                data_row.get('bollinger_inferior', ''),
                data_row.get('bollinger_superior', ''),
                data_row.get('bollinger_decision', ''),
                data_row.get('macd_line', ''),
                data_row.get('signal_line', ''),
                data_row.get('histogram', ''),
                data_row.get('macd_decision', ''),
                data_row.get('rs', ''),
                data_row.get('rsi', ''),
                data_row.get('rsi_decision', ''),
                data_row.get('stochastic_k', ''),
                data_row.get('stochastic_d', ''),
                data_row.get('stochastic_decision', '')
            ]
            
            # Convertir les valeurs en chaînes, remplacer NaN par vide
            values = ['' if pd.isna(v) else (f"{v:.2f}" if isinstance(v, (int, float)) else str(v)) for v in values]
            
            # Plage F à X pour cette ligne
            range_name = f"F{row_num}:X{row_num}"
            updates.append({
                'range': range_name,
                'values': [values]
            })
        
        if not updates:
            logging.warning(f"   ⚠️  Aucune mise à jour pour {symbol}")
            return False
        
        # Mise à jour par lot (batch update)
        worksheet.batch_update(updates, value_input_option='USER_ENTERED')
        
        logging.info(f"   ✅ {len(updates)} ligne(s) mises à jour dans GSheet")
        return True
    
    except gspread.exceptions.WorksheetNotFound:
        logging.warning(f"   ⚠️  Feuille '{symbol}' non trouvée")
        return False
    
    except Exception as e:
        logging.error(f"   ❌ Erreur GSheet pour {symbol}: {e}")
        return False

# --- Traitement par société ---
def process_company(conn, gc, spreadsheet, company_id, symbol):
    logging.info(f"--- Traitement: {symbol} ---")
    
    # Récupérer les données historiques
    query = "SELECT id, trade_date, price FROM historical_data WHERE company_id = %s ORDER BY trade_date;"
    df = pd.read_sql(query, conn, params=(company_id,), index_col='id')
    
    if len(df) < 50:
        logging.warning(f"   ⚠️  Pas assez de données ({len(df)} lignes)")
        return 0
    
    # Calcul des indicateurs
    df = calculate_moving_averages(df)
    df = calculate_bollinger_bands(df)
    df
