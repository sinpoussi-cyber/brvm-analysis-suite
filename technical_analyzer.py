# ==============================================================================
# MODULE: TECHNICAL ANALYZER
# Description: Calcule les indicateurs techniques pour chaque société.
# ==============================================================================

# --- Imports ---
import gspread
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import warnings
import re
import os
import json
import time
import logging

warnings.filterwarnings('ignore')

# --- Authentification ---
def authenticate_gsheets():
    try:
        logging.info("Authentification via compte de service Google...")
        creds_json_str = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
        if not creds_json_str:
            logging.error("❌ Secret GSPREAD_SERVICE_ACCOUNT introuvable.")
            return None
        creds_dict = json.loads(creds_json_str)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur d'authentification : {e}")
        return None

def clean_numeric_value(value):
    if pd.isna(value) or value == '' or value is None: return np.nan
    str_value = re.sub(r'[^\d.,\-+]', '', str(value).strip()).replace(',', '.')
    try:
        return float(str_value)
    except (ValueError, TypeError):
        return np.nan

def convert_columns_to_numeric(gc, spreadsheet_id, sheet_name):
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        logging.info(f"Conversion des données numériques pour {sheet_name}...")
        all_values = worksheet.get_all_values()

        if len(all_values) < 2:
            logging.warning(f"Pas assez de données dans {sheet_name}")
            return False

        headers = all_values[0]
        data = all_values[1:]
        
        updates = []
        for col_index, col_letter in [(2, 'C'), (3, 'D'), (4, 'E')]:
            if col_index < len(headers):
                numeric_values = [[clean_numeric_value(row[col_index]) if col_index < len(row) else ""] for row in data]
                updates.append({'range': f'{col_letter}2:{col_letter}{len(data) + 1}', 'values': numeric_values})

        if updates:
            worksheet.batch_update(updates, value_input_option='USER_ENTERED')
            logging.info(f"  ✓ Colonnes converties pour {sheet_name}")
        return True
    except Exception as e:
        logging.error(f"  ✗ Erreur de conversion pour {sheet_name}: {e}")
        return False

# --- Fonctions de calcul des indicateurs ---
def calculate_moving_averages(df, price_col):
    df['MM5'] = df[price_col].rolling(window=5).mean()
    df['MM10'] = df[price_col].rolling(window=10).mean()
    df['MM20'] = df[price_col].rolling(window=20).mean()
    df['MM50'] = df[price_col].rolling(window=50).mean()
    def mm_decision(row):
        price, mm5, mm10, mm20, mm50 = row[price_col], row['MM5'], row['MM10'], row['MM20'], row['MM50']
        if any(pd.isna(val) for val in [price, mm5, mm10, mm20, mm50]): return "Attendre"
        if ((price > mm5) and (mm5 > mm10)) or ((mm5 > mm10) and (mm10 > mm20)) or ((mm10 > mm20) and (mm20 > mm50)):
            return "Achat"
        return "Vente"
    df['MMdecision'] = df.apply(mm_decision, axis=1)
    return df

def calculate_bollinger_bands(df, price_col, window=35, num_std=2):
    df['Bande_centrale'] = df[price_col].rolling(window=window).mean()
    rolling_std = df[price_col].rolling(window=window).std()
    df['Bande_Supérieure'] = df['Bande_centrale'] + (rolling_std * num_std)
    df['Bande_Inferieure'] = df['Bande_centrale'] - (rolling_std * num_std)
    def bollinger_decision(row):
        price, lower, upper = row[price_col], row['Bande_Inferieure'], row['Bande_Supérieure']
        if any(pd.isna(val) for val in [price, lower, upper]): return "Attendre"
        if price <= lower: return "Achat"
        if price >= upper: return "Vente"
        return "Neutre"
    df['Boldecision'] = df.apply(bollinger_decision, axis=1)
    return df

def calculate_macd(df, price_col, fast=12, slow=26, signal=9):
    df['MME_fast'] = df[price_col].ewm(span=fast, adjust=False).mean()
    df['MME_slow'] = df[price_col].ewm(span=slow, adjust=False).mean()
    df['Ligne MACD'] = df['MME_fast'] - df['MME_slow']
    df['Ligne de signal'] = df['Ligne MACD'].ewm(span=signal, adjust=False).mean()
    df['Histogramme'] = df['Ligne MACD'] - df['Ligne de signal']
    df['prev_histo'] = df['Histogramme'].shift(1)
    def macd_decision(row):
        if pd.isna(row['Histogramme']) or pd.isna(row['prev_histo']): return "Attendre"
        if row['prev_histo'] <= 0 and row['Histogramme'] > 0: return "Achat (Fort)"
        if row['prev_histo'] >= 0 and row['Histogramme'] < 0: return "Vente (Fort)"
        if row['Histogramme'] > 0: return "Achat"
        if row['Histogramme'] < 0: return "Vente"
        return "Neutre"
    df['MACDdecision'] = df.apply(macd_decision, axis=1)
    return df

def calculate_rsi(df, price_col, period=20):
    delta = df[price_col].diff(1)
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.where(delta < 0, 0).ewm(alpha=1/period, adjust=False).mean()
    df['RS'] = gain / loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + df['RS']))
    df['prev_rsi'] = df['RSI'].shift(1)
    def rsi_decision(row):
        if pd.isna(row['RSI']) or pd.isna(row['prev_rsi']): return "Attendre"
        if row['prev_rsi'] <= 30 and row['RSI'] > 30: return "Achat"
        if row['prev_rsi'] >= 70 and row['RSI'] < 70: return "Vente"
        return "Neutre"
    df['RSIdecision'] = df.apply(rsi_decision, axis=1)
    return df

def calculate_stochastic(df, price_col, k_period=20, d_period=5):
    rolling_high = df[price_col].rolling(window=k_period).max()
    rolling_low = df[price_col].rolling(window=k_period).min()
    df['%K'] = 100 * ((df[price_col] - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan))
    df['%D'] = df['%K'].rolling(window=d_period).mean()
    df['prev_%K'] = df['%K'].shift(1)
    df['prev_%D'] = df['%D'].shift(1)
    def stochastic_decision(row):
        if any(pd.isna(val) for val in [row['%K'], row['%D'], row['prev_%K'], row['prev_%D']]): return "Attendre"
        if row['prev_%K'] <= row['prev_%D'] and row['%K'] > row['%D'] and row['%D'] < 20: return "Achat (Fort)"
        if row['prev_%K'] >= row['prev_%D'] and row['%K'] < row['%D'] and row['%D'] > 80: return "Vente (Fort)"
        return "Neutre"
    df['Stocdecision'] = df.apply(stochastic_decision, axis=1)
    return df

def process_single_sheet(gc, spreadsheet_id, sheet_name):
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        all_values = worksheet.get_all_values()
        if not all_values or len(all_values) < 2:
            logging.warning(f"  La feuille {sheet_name} est vide ou n'a pas d'en-tête.")
            return

        headers = all_values[0]
        data = all_values[1:]
        
        valid_columns_indices = {i: h for i, h in enumerate(headers) if h.strip()}
        filtered_data = [[row[i] for i in valid_columns_indices] if len(row) > max(valid_columns_indices) else [row[i] for i in valid_columns_indices if i < len(row)] for row in data]
        df = pd.DataFrame(filtered_data, columns=list(valid_columns_indices.values()))

        price_col = 'Cours (F CFA)'
        if price_col not in df.columns:
            logging.error(f"  ✗ Colonne '{price_col}' introuvable dans {sheet_name}")
            return

        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
            df = df.sort_values('Date').reset_index(drop=True)

        df.dropna(subset=[price_col], inplace=True)
        if len(df) < 50:
            logging.warning(f"  ✗ Pas assez de données ({len(df)} lignes) pour {sheet_name}.")
            return

        df = calculate_moving_averages(df, price_col)
        df = calculate_bollinger_bands(df, price_col)
        df = calculate_macd(df, price_col)
        df = calculate_rsi(df, price_col)
        df = calculate_stochastic(df, price_col)
        
        headers_to_write = ['MM5','MM10','MM20','MM50','MMdecision','Bande_centrale','Bande_Inferieure','Bande_Supérieure','Boldecision','Ligne MACD','Ligne de signal','Histogramme','MACDdecision','RS','RSI','RSIdecision','%K','%D','Stocdecision']
        worksheet.update('F1:X1', [headers_to_write])
        
        df_to_write = df[headers_to_write].copy()
        for col in ['MM5','MM10','MM20','MM50','Bande_centrale','Bande_Supérieure','Bande_Inferieure','Ligne MACD','Ligne de signal','Histogramme','RS','RSI','%K','%D']:
            df_to_write[col] = df_to_write[col].round(2)
        
        df_to_write.fillna('', inplace=True)
        worksheet.update(f'F2:X{len(df_to_write)+1}', df_to_write.values.tolist())
        
        logging.info(f"  ✓ Traitement terminé pour {sheet_name}")
    except Exception as e:
        logging.error(f"  ✗ Erreur lors du traitement de {sheet_name}: {e}")

def run_technical_analysis():
    logging.info("="*60)
    logging.info("ÉTAPE 3 : DÉMARRAGE DE L'ANALYSE TECHNIQUE")
    logging.info("="*60)
    
    spreadsheet_id = "1EGXyg13ml8a9zr4OaUPnJN3i-rwVO2uq330yfxJXnSM"
    gc = authenticate_gsheets()
    if not gc: return

    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        logging.info(f"Fichier ouvert: {spreadsheet.title}")

        sheet_names = [ws.title for ws in spreadsheet.worksheets() if ws.title not in ["UNMATCHED", "Actions_BRVM"]]
        logging.info(f"Feuilles à traiter: {sheet_names}")

        for sheet_name in sheet_names:
            logging.info(f"\n--- TRAITEMENT DE LA FEUILLE: {sheet_name} ---")
            time.sleep(5)
            convert_columns_to_numeric(gc, spreadsheet_id, sheet_name)
            time.sleep(5)
            process_single_sheet(gc, spreadsheet_id, sheet_name)

    except Exception as e:
        logging.error(f"Erreur générale: {e}")
    
    logging.info("Processus d'analyse technique terminé.")
