# ==============================================================================
# MODULE: TECHNICAL ANALYZER (V3.2 - VERSION CORRIGÉE)
# ==============================================================================

import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import warnings
import os
import logging
import time

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
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("✅ Connexion à la base de données pour l'analyse technique réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à la base de données : {e}")
        return None

def calculate_moving_averages(df, price_col='price'):
    """Calcule les moyennes mobiles et génère un signal."""
    df['mm5'] = df[price_col].rolling(window=5).mean()
    df['mm10'] = df[price_col].rolling(window=10).mean()
    df['mm20'] = df[price_col].rolling(window=20).mean()
    df['mm50'] = df[price_col].rolling(window=50).mean()
    
    def mm_decision(row):
        price, mm5, mm10, mm20, mm50 = row[price_col], row['mm5'], row['mm10'], row['mm20'], row['mm50']
        
        if any(pd.isna(val) for val in [price, mm5, mm10, mm20, mm50]):
            return "Attendre"
        
        # Signal d'achat si tendance haussière
        if ((price > mm5) and (mm5 > mm10)) or ((mm5 > mm10) and (mm10 > mm20)) or ((mm10 > mm20) and (mm20 > mm50)):
            return "Achat"
        
        return "Vente"
    
    df['mm_decision'] = df.apply(mm_decision, axis=1)
    return df

def calculate_bollinger_bands(df, price_col='price', window=35, num_std=2):
    """Calcule les bandes de Bollinger et génère un signal."""
    df['bollinger_central'] = df[price_col].rolling(window=window).mean()
    rolling_std = df[price_col].rolling(window=window).std()
    df['bollinger_superior'] = df['bollinger_central'] + (rolling_std * num_std)
    df['bollinger_inferior'] = df['bollinger_central'] - (rolling_std * num_std)
    
    def bollinger_decision(row):
        price = row[price_col]
        lower = row['bollinger_inferior']
        upper = row['bollinger_superior']
        
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
    """Calcule le MACD et génère un signal."""
    df['macd_line'] = df[price_col].ewm(span=fast, adjust=False).mean() - df[price_col].ewm(span=slow, adjust=False).mean()
    df['signal_line'] = df['macd_line'].ewm(span=signal, adjust=False).mean()
    df['histogram'] = df['macd_line'] - df['signal_line']
    df['prev_histo'] = df['histogram'].shift(1)
    
    def macd_decision(row):
        if pd.isna(row['histogram']) or pd.isna(row['prev_histo']):
            return "Attendre"
        
        # Croisement haussier
        if row['prev_histo'] <= 0 and row['histogram'] > 0:
            return "Achat (Fort)"
        
        # Croisement baissier
        if row['prev_histo'] >= 0 and row['histogram'] < 0:
            return "Vente (Fort)"
        
        # Tendance
        if row['histogram'] > 0:
            return "Achat"
        if row['histogram'] < 0:
            return "Vente"
        
        return "Neutre"
    
    df['macd_decision'] = df.apply(macd_decision, axis=1)
    return df

def calculate_rsi(df, price_col='price', period=20):
    """Calcule le RSI et génère un signal."""
    delta = df[price_col].diff(1)
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.where(delta < 0, 0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi'] = 100 - (100 / (1 + rs))
    df['prev_rsi'] = df['rsi'].shift(1)
    
    def rsi_decision(row):
        if pd.isna(row['rsi']) or pd.isna(row['prev_rsi']):
            return "Attendre"
        
        # Sortie de zone de survente
        if row['prev_rsi'] <= 30 and row['rsi'] > 30:
            return "Achat"
        
        # Sortie de zone de surachat
        if row['prev_rsi'] >= 70 and row['rsi'] < 70:
            return "Vente"
        
        return "Neutre"
    
    df['rsi_decision'] = df.apply(rsi_decision, axis=1)
    return df

def calculate_stochastic(df, price_col='price', k_period=20, d_period=5):
    """Calcule l'oscillateur stochastique et génère un signal."""
    rolling_high = df[price_col].rolling(window=k_period).max()
    rolling_low = df[price_col].rolling(window=k_period).min()
    df['stochastic_k'] = 100 * ((df[price_col] - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan))
    df['stochastic_d'] = df['stochastic_k'].rolling(window=d_period).mean()
    df['prev_k'] = df['stochastic_k'].shift(1)
    df['prev_d'] = df['stochastic_d'].shift(1)
    
    def stochastic_decision(row):
        if any(pd.isna(val) for val in [row['stochastic_k'], row['stochastic_d'], row['prev_k'], row['prev_d']]):
            return "Attendre"
        
        # Croisement haussier en zone de survente
        if row['prev_k'] <= row['prev_d'] and row['stochastic_k'] > row['stochastic_d'] and row['stochastic_d'] < 20:
            return "Achat (Fort)"
        
        # Croisement baissier en zone de surachat
        if row['prev_k'] >= row['prev_d'] and row['stochastic_k'] < row['stochastic_d'] and row['stochastic_d'] > 80:
            return "Vente (Fort)"
        
        return "Neutre"
    
    df['stochastic_decision'] = df.apply(stochastic_decision, axis=1)
    return df

def process_company(conn, company_id, company_symbol):
    """Récupère les données, calcule les indicateurs et met à jour la DB pour une société."""
    logging.info(f"--- Traitement de l'analyse technique pour : {company_symbol} ---")
    
    # Récupérer les données historiques
    query = "SELECT id, trade_date, price FROM historical_data WHERE company_id = %s ORDER BY trade_date;"
    df = pd.read_sql(query, conn, params=(company_id,), index_col='id')
    
    if len(df) < 50:
        logging.warning(f"  ⚠️  Pas assez de données ({len(df)} lignes) pour {company_symbol}. Analyse ignorée.")
        return 0
    
    # Calculer tous les indicateurs
    df = calculate_moving_averages(df)
    df = calculate_bollinger_bands(df)
    df = calculate_macd(df)
    df = calculate_rsi(df)
    df = calculate_stochastic(df)
    
    # Préparer les données pour la mise à jour
    df_to_update = df.drop(columns=['trade_date', 'price', 'prev_histo', 'prev_rsi', 'prev_k', 'prev_d']).reset_index()
    df_to_update.rename(columns={'id': 'historical_data_id'}, inplace=True)
    df_to_update = df_to_update.replace({np.nan: None})
    
    # Insérer ou mettre à jour dans la base
    cur = conn.cursor()
    update_count = 0
    
    for index, row in df_to_update.iterrows():
        query = sql.SQL("""
            INSERT INTO technical_analysis (
                historical_data_id, mm5, mm10, mm20, mm50, mm_decision,
                bollinger_central, bollinger_inferior, bollinger_superior, bollinger_decision,
                macd_line, signal_line, histogram, macd_decision,
                rsi, rsi_decision,
                stochastic_k, stochastic_d, stochastic_decision
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (historical_data_id) DO UPDATE SET
                mm5 = EXCLUDED.mm5,
                mm10 = EXCLUDED.mm10,
                mm20 = EXCLUDED.mm20,
                mm50 = EXCLUDED.mm50,
                mm_decision = EXCLUDED.mm_decision,
                bollinger_central = EXCLUDED.bollinger_central,
                bollinger_inferior = EXCLUDED.bollinger_inferior,
                bollinger_superior = EXCLUDED.bollinger_superior,
                bollinger_decision = EXCLUDED.bollinger_decision,
                macd_line = EXCLUDED.macd_line,
                signal_line = EXCLUDED.signal_line,
                histogram = EXCLUDED.histogram,
                macd_decision = EXCLUDED.macd_decision,
                rsi = EXCLUDED.rsi,
                rsi_decision = EXCLUDED.rsi_decision,
                stochastic_k = EXCLUDED.stochastic_k,
                stochastic_d = EXCLUDED.stochastic_d,
                stochastic_decision = EXCLUDED.stochastic_decision;
        """)
        
        cur.execute(query, (
            row['historical_data_id'],
            row.get('mm5'), row.get('mm10'), row.get('mm20'), row.get('mm50'), row.get('mm_decision'),
            row.get('bollinger_central'), row.get('bollinger_inferior'), row.get('bollinger_superior'), row.get('bollinger_decision'),
            row.get('macd_line'), row.get('signal_line'), row.get('histogram'), row.get('macd_decision'),
            row.get('rsi'), row.get('rsi_decision'),
            row.get('stochastic_k'), row.get('stochastic_d'), row.get('stochastic_decision')
        ))
        update_count += cur.rowcount
    
    conn.commit()
    cur.close()
    
    logging.info(f"  ✅ Analyse technique terminée pour {company_symbol}. {update_count} enregistrements mis à jour.")
    return update_count

def run_technical_analysis():
    """Fonction principale pour lancer l'analyse technique sur toutes les sociétés."""
    logging.info("=" * 60)
    logging.info("ÉTAPE 2 : DÉMARRAGE DE L'ANALYSE TECHNIQUE (VERSION POSTGRESQL)")
    logging.info("=" * 60)
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # Récupérer la liste des sociétés
        cur = conn.cursor()
        cur.execute("SELECT id, symbol FROM companies ORDER BY symbol;")
        companies = cur.fetchall()
        cur.close()
        
        logging.info(f"📊 {len(companies)} sociétés à analyser.")
        
        total_updates = 0
        companies_analyzed = 0
        
        for company_id, company_symbol in companies:
            updates = process_company(conn, company_id, company_symbol)
            if updates > 0:
                companies_analyzed += 1
                total_updates += updates
            time.sleep(0.5)  # Petit délai entre chaque société
        
        # Résumé
        logging.info("\n" + "=" * 60)
        logging.info("📊 RÉSUMÉ DE L'ANALYSE TECHNIQUE")
        logging.info("=" * 60)
        logging.info(f"   • Sociétés traitées : {len(companies)}")
        logging.info(f"   • Sociétés analysées : {companies_analyzed}")
        logging.info(f"   • Total mises à jour : {total_updates}")
        logging.info("=" * 60)
        logging.info("✅ Analyse technique terminée avec succès")
    
    except Exception as e:
        logging.error(f"❌ Erreur critique lors de l'analyse technique : {e}", exc_info=True)
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_technical_analysis()
