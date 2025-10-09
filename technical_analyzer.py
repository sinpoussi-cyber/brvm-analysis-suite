# ==============================================================================
# MODULE: TECHNICAL ANALYZER (V6.2 - SYNCHRONISATION DB + GOOGLE SHEETS)
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
from collections import deque
from datetime import datetime, timedelta

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# --- Rate Limiter ---
class SmartRateLimiter:
    def __init__(self, requests_per_minute=15):
        self.rpm = requests_per_minute
        self.requests = deque()
    
    def wait_if_needed(self):
        now = datetime.now()
        minute_ago = now - timedelta(seconds=60)
        
        while self.requests and self.requests[0] < minute_ago:
            self.requests.popleft()
        
        if len(self.requests) >= self.rpm:
            sleep_until = self.requests[0] + timedelta(seconds=60)
            sleep_time = (sleep_until - now).total_seconds()
            if sleep_time > 0:
                logging.warning(f"⏸️  Pause de {sleep_time:.1f}s pour respecter le rate limit")
                time.sleep(sleep_time + 0.1)
                self.requests.popleft()
        
        self.requests.append(now)

# --- Connexion DB ---
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD, 
            host=DB_HOST, 
            port=DB_PORT
        )
        logging.info("✅ Connexion PostgreSQL pour analyse technique réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à la DB : {e}")
        return None

# --- Authentification Google Sheets ---
def authenticate_gsheets():
    try:
        creds_json = GSPREAD_SERVICE_ACCOUNT_JSON
        
        if not creds_json:
            logging.warning("⚠️  GSPREAD_SERVICE_ACCOUNT non défini")
            return None
        
        if not creds_json.strip().startswith('{'):
            logging.error("❌ GSPREAD_SERVICE_ACCOUNT ne contient pas un JSON valide")
            return None
        
        creds_dict = json.loads(creds_json)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google Sheets réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur d'authentification Google Sheets : {e}")
        return None

# --- Calcul des indicateurs ---
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

def calculate_rsi(df, price_col='price', period=20):
    delta = df[price_col].diff(1)
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.where(delta < 0, 0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi'] = 100 - (100 / (1 + rs))
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

# --- Écriture dans Google Sheets (Analyse Technique) ---
def write_technical_to_gsheet(gc, symbol, df):
    """Écrit l'analyse technique dans Google Sheets."""
    if not gc or not SPREADSHEET_ID:
        return False
    
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        try:
            worksheet = spreadsheet.worksheet(f"{symbol}_TECHNICAL")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=f"{symbol}_TECHNICAL", rows=1000, cols=20)
            headers = ['Date', 'Price', 'MM5', 'MM10', 'MM20', 'MM50', 'MM_Decision', 
                      'Bollinger_Central', 'Bollinger_Inferior', 'Bollinger_Superior', 'Bollinger_Decision',
                      'MACD_Line', 'Signal_Line', 'Histogram', 'MACD_Decision',
                      'RSI', 'RSI_Decision', 'Stochastic_K', 'Stochastic_D', 'Stochastic_Decision']
            worksheet.update([headers])
            logging.info(f"  📄 Feuille '{symbol}_TECHNICAL' créée")
        
        # Préparer les données (dernières 50 lignes)
        df_export = df.tail(50).copy()
        df_export['trade_date'] = df_export['trade_date'].dt.strftime('%Y-%m-%d')
        
        cols = ['trade_date', 'price', 'mm5', 'mm10', 'mm20', 'mm50', 'mm_decision',
                'bollinger_central', 'bollinger_inferior', 'bollinger_superior', 'bollinger_decision',
                'macd_line', 'signal_line', 'histogram', 'macd_decision',
                'rsi', 'rsi_decision', 'stochastic_k', 'stochastic_d', 'stochastic_decision']
        
        data_to_write = df_export[cols].fillna('').values.tolist()
        
        # Effacer et réécrire toutes les données
        worksheet.clear()
        headers = ['Date', 'Price', 'MM5', 'MM10', 'MM20', 'MM50', 'MM_Decision', 
                  'Bollinger_Central', 'Bollinger_Inferior', 'Bollinger_Superior', 'Bollinger_Decision',
                  'MACD_Line', 'Signal_Line', 'Histogram', 'MACD_Decision',
                  'RSI', 'RSI_Decision', 'Stochastic_K', 'Stochastic_D', 'Stochastic_Decision']
        worksheet.update([headers] + data_to_write)
        
        return True
    except Exception as e:
        logging.error(f"  ❌ Erreur GSheets technique pour {symbol}: {e}")
        return False

# --- Traitement par société ---
def process_company(conn, gc, company_id, company_symbol):
    logging.info(f"--- Traitement : {company_symbol} ---")
    
    query = "SELECT id, trade_date, price FROM historical_data WHERE company_id = %s ORDER BY trade_date;"
    df = pd.read_sql(query, conn, params=(company_id,), index_col='id')
    
    if len(df) < 50:
        logging.warning(f"  -> Pas assez de données ({len(df)} lignes)")
        return 0

    df = calculate_moving_averages(df)
    df = calculate_bollinger_bands(df)
    df = calculate_macd(df)
    df = calculate_rsi(df)
    df = calculate_stochastic(df)
    
    # Préparer les données pour PostgreSQL
    df_to_update = df.drop(columns=['prev_histo', 'prev_rsi', 'prev_k', 'prev_d'], errors='ignore').reset_index()
    df_to_update.rename(columns={'id': 'historical_data_id'}, inplace=True)
    df_to_update = df_to_update.replace({np.nan: None})
    
    # 1️⃣ ÉCRITURE DANS POSTGRESQL
    cur = conn.cursor()
    update_count = 0
    
    for index, row in df_to_update.iterrows():
        query = sql.SQL("""
            INSERT INTO technical_analysis (
                historical_data_id, mm5, mm10, mm20, mm50, mm_decision, 
                bollinger_central, bollinger_inferior, bollinger_superior, bollinger_decision, 
                macd_line, signal_line, histogram, macd_decision, 
                rsi, rsi_decision, stochastic_k, stochastic_d, stochastic_decision
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (historical_data_id) DO UPDATE SET
                mm5 = EXCLUDED.mm5, mm10 = EXCLUDED.mm10, mm20 = EXCLUDED.mm20, mm50 = EXCLUDED.mm50, 
                mm_decision = EXCLUDED.mm_decision,
                bollinger_central = EXCLUDED.bollinger_central, bollinger_inferior = EXCLUDED.bollinger_inferior, 
                bollinger_superior = EXCLUDED.bollinger_superior, bollinger_decision = EXCLUDED.bollinger_decision,
                macd_line = EXCLUDED.macd_line, signal_line = EXCLUDED.signal_line, histogram = EXCLUDED.histogram, 
                macd_decision = EXCLUDED.macd_decision,
                rsi = EXCLUDED.rsi, rsi_decision = EXCLUDED.rsi_decision, 
                stochastic_k = EXCLUDED.stochastic_k, stochastic_d = EXCLUDED.stochastic_d, 
                stochastic_decision = EXCLUDED.stochastic_decision;
        """)
        
        cur.execute(query, (
            row['historical_data_id'], row.get('mm5'), row.get('mm10'), row.get('mm20'), row.get('mm50'), 
            row.get('mm_decision'),
            row.get('bollinger_central'), row.get('bollinger_inferior'), row.get('bollinger_superior'), 
            row.get('bollinger_decision'),
            row.get('macd_line'), row.get('signal_line'), row.get('histogram'), row.get('macd_decision'),
            row.get('rsi'), row.get('rsi_decision'), 
            row.get('stochastic_k'), row.get('stochastic_d'), row.get('stochastic_decision')
        ))
        update_count += cur.rowcount
        
    conn.commit()
    cur.close()
    logging.info(f"  ✅ PostgreSQL: {update_count} enregistrements")
    
    # 2️⃣ ÉCRITURE DANS GOOGLE SHEETS (simultanée)
    if gc:
        if write_technical_to_gsheet(gc, company_symbol, df):
            logging.info(f"  ✅ Google Sheets: données techniques synchronisées")
    
    time.sleep(0.3)
    return update_count

# --- Fonction principale ---
def run_technical_analysis():
    logging.info("="*80)
    logging.info("ÉTAPE 2 : ANALYSE TECHNIQUE (V6.2 - SYNC DB + GSHEETS)")
    logging.info("="*80)
    
    start_time = time.time()
    conn = connect_to_db()
    if not conn: 
        return

    gc = authenticate_gsheets()
    if not gc:
        logging.warning("⚠️  Google Sheets non disponible, PostgreSQL uniquement")

    try:
        cur = conn.cursor()
        cur.execute("SELECT id, symbol FROM companies;")
        companies = cur.fetchall()
        cur.close()

        logging.info(f"📊 {len(companies)} sociétés à analyser")
        
        total_updates = 0
        for company_id, company_symbol in companies:
            total_updates += process_company(conn, gc, company_id, company_symbol)

        elapsed = time.time() - start_time
        logging.info("\n" + "="*80)
        logging.info(f"✅ Analyse technique terminée en {elapsed:.2f}s")
        logging.info(f"📊 Total: {total_updates} mises à jour")
        logging.info("="*80)

    except Exception as e:
        logging.error(f"❌ Erreur critique : {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_technical_analysis()
