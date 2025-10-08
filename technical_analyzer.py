# ==============================================================================
# MODULE: TECHNICAL ANALYZER (V5.0 - BATCH PROCESSING OPTIMISÉ)
# ==============================================================================
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import warnings
import os
import logging
import time

# Import du gestionnaire de synchronisation optimisé
from sync_data_manager import SyncDataManager

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Secrets de la base de données ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

def connect_to_db():
    """Établit la connexion à la base de données PostgreSQL."""
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        logging.info("✅ Connexion à la base de données pour l'analyse technique réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à la base de données : {e}")
        return None

def calculate_moving_averages(df, price_col='price'):
    df['mm5'] = df[price_col].rolling(window=5).mean()
    df['mm10'] = df[price_col].rolling(window=10).mean()
    df['mm20'] = df[price_col].rolling(window=20).mean()
    df['mm50'] = df[price_col].rolling(window=50).mean()
    def mm_decision(row):
        price, mm5, mm10, mm20, mm50 = row[price_col], row['mm5'], row['mm10'], row['mm20'], row['mm50']
        if any(pd.isna(val) for val in [price, mm5, mm10, mm20, mm50]): return "Attendre"
        if ((price > mm5) and (mm5 > mm10)) or ((mm5 > mm10) and (mm10 > mm20)) or ((mm10 > mm20) and (mm20 > mm50)): return "Achat"
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
        if any(pd.isna(val) for val in [price, lower, upper]): return "Attendre"
        if price <= lower: return "Achat"
        if price >= upper: return "Vente"
        return "Neutre"
    df['bollinger_decision'] = df.apply(bollinger_decision, axis=1)
    return df

def calculate_macd(df, price_col='price', fast=12, slow=26, signal=9):
    df['macd_line'] = df[price_col].ewm(span=fast, adjust=False).mean() - df[price_col].ewm(span=slow, adjust=False).mean()
    df['signal_line'] = df['macd_line'].ewm(span=signal, adjust=False).mean()
    df['histogram'] = df['macd_line'] - df['signal_line']
    df['prev_histo'] = df['histogram'].shift(1)
    def macd_decision(row):
        if pd.isna(row['histogram']) or pd.isna(row['prev_histo']): return "Attendre"
        if row['prev_histo'] <= 0 and row['histogram'] > 0: return "Achat (Fort)"
        if row['prev_histo'] >= 0 and row['histogram'] < 0: return "Vente (Fort)"
        if row['histogram'] > 0: return "Achat"
        if row['histogram'] < 0: return "Vente"
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
        if pd.isna(row['rsi']) or pd.isna(row['prev_rsi']): return "Attendre"
        if row['prev_rsi'] <= 30 and row['rsi'] > 30: return "Achat"
        if row['prev_rsi'] >= 70 and row['rsi'] < 70: return "Vente"
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
        if any(pd.isna(val) for val in [row['stochastic_k'], row['stochastic_d'], row['prev_k'], row['prev_d']]): return "Attendre"
        if row['prev_k'] <= row['prev_d'] and row['stochastic_k'] > row['stochastic_d'] and row['stochastic_d'] < 20: return "Achat (Fort)"
        if row['prev_k'] >= row['prev_d'] and row['stochastic_k'] < row['stochastic_d'] and row['stochastic_d'] > 80: return "Vente (Fort)"
        return "Neutre"
    df['stochastic_decision'] = df.apply(stochastic_decision, axis=1)
    return df

def process_company(conn, sync_manager, company_id, company_symbol):
    """Récupère les données, calcule les indicateurs et ajoute au batch."""
    logging.info(f"--- Traitement de l'analyse technique pour : {company_symbol} ---")
    
    query = "SELECT id, trade_date, price FROM historical_data WHERE company_id = %s ORDER BY trade_date;"
    df = pd.read_sql(query, conn, params=(company_id,), index_col='id')
    
    if len(df) < 50:
        logging.warning(f"  -> Pas assez de données ({len(df)} lignes) pour {company_symbol}. Analyse ignorée.")
        return 0

    df = calculate_moving_averages(df)
    df = calculate_bollinger_bands(df)
    df = calculate_macd(df)
    df = calculate_rsi(df)
    df = calculate_stochastic(df)
    
    df_to_update = df.drop(columns=['trade_date', 'price', 'prev_histo', 'prev_rsi', 'prev_k', 'prev_d']).reset_index()
    df_to_update.rename(columns={'id': 'historical_data_id'}, inplace=True)
    df_to_update = df_to_update.replace({np.nan: None})
    
    # AJOUT AU BATCH au lieu d'écriture immédiate
    for index, row in df_to_update.iterrows():
        analysis_data = {
            'mm5': row.get('mm5'),
            'mm10': row.get('mm10'),
            'mm20': row.get('mm20'),
            'mm50': row.get('mm50'),
            'mm_decision': row.get('mm_decision'),
            'bollinger_central': row.get('bollinger_central'),
            'bollinger_inferior': row.get('bollinger_inferior'),
            'bollinger_superior': row.get('bollinger_superior'),
            'bollinger_decision': row.get('bollinger_decision'),
            'macd_line': row.get('macd_line'),
            'signal_line': row.get('signal_line'),
            'histogram': row.get('histogram'),
            'macd_decision': row.get('macd_decision'),
            'rsi': row.get('rsi'),
            'rsi_decision': row.get('rsi_decision'),
            'stochastic_k': row.get('stochastic_k'),
            'stochastic_d': row.get('stochastic_d'),
            'stochastic_decision': row.get('stochastic_decision')
        }
        
        # Ajouter au batch (pas d'écriture immédiate)
        sync_manager.add_to_technical_batch(
            symbol=company_symbol,
            historical_data_id=row['historical_data_id'],
            analysis_data=analysis_data
        )
    
    logging.info(f"  -> {len(df_to_update)} analyses ajoutées au batch pour {company_symbol}.")
    return len(df_to_update)

def run_technical_analysis():
    """Fonction principale pour lancer l'analyse technique sur toutes les sociétés."""
    logging.info("="*60)
    logging.info("ÉTAPE 2 : DÉMARRAGE DE L'ANALYSE TECHNIQUE (BATCH PROCESSING)")
    logging.info("="*60)
    
    conn = connect_to_db()
    if not conn: return
    
    # Initialiser le gestionnaire de synchronisation
    sync_manager = SyncDataManager()

    try:
        cur = conn.cursor()
        cur.execute("SELECT id, symbol FROM companies;")
        companies = cur.fetchall()
        cur.close()

        logging.info(f"{len(companies)} sociétés à analyser.")
        
        total_analyses = 0
        for company_id, company_symbol in companies:
            analyses_count = process_company(conn, sync_manager, company_id, company_symbol)
            total_analyses += analyses_count
            
            # FLUSH le batch après chaque société pour éviter trop de données en mémoire
            try:
                logging.info(f"🚀 Flush du batch pour {company_symbol}...")
                sync_manager.flush_technical_batch()
                logging.info(f"✅ Batch pour {company_symbol} synchronisé avec succès.")
            except Exception as e:
                logging.error(f"❌ Erreur lors du flush du batch pour {company_symbol}: {e}")
                # Continuer avec la société suivante
                continue
            
            # Pause entre les sociétés pour respecter les quotas
            time.sleep(2)

        logging.info(f"✅ Analyse technique terminée. {total_analyses} analyses traitées au total.")

    except Exception as e:
        logging.error(f"❌ Erreur critique lors de l'analyse technique : {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
        sync_manager.close()

if __name__ == "__main__":
    run_technical_analysis()
