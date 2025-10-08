# ==============================================================================
# MODULE: TECHNICAL ANALYZER (V6.0 - BATCH OPTIMISÉ + GOOGLE SHEETS SYNC)
# ==============================================================================
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import pandas as pd
import numpy as np
import warnings
import os
import logging
import time
import gspread
from google.oauth2 import service_account
import json

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

# Taille des batchs pour optimisation
BATCH_SIZE = 1000

def connect_to_db():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        logging.info("✅ Connexion PostgreSQL pour analyse technique réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à PostgreSQL : {e}")
        return None

def authenticate_gsheets():
    try:
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google Sheets réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur d'authentification Google Sheets : {e}")
        return None

def calculate_moving_averages(df, price_col='price'):
    """Calcule les moyennes mobiles avec vectorisation."""
    df['mm5'] = df[price_col].rolling(window=5, min_periods=1).mean()
    df['mm10'] = df[price_col].rolling(window=10, min_periods=1).mean()
    df['mm20'] = df[price_col].rolling(window=20, min_periods=1).mean()
    df['mm50'] = df[price_col].rolling(window=50, min_periods=1).mean()
    
    # Décision vectorisée
    conditions = [
        ((df[price_col] > df['mm5']) & (df['mm5'] > df['mm10'])) |
        ((df['mm5'] > df['mm10']) & (df['mm10'] > df['mm20'])) |
        ((df['mm10'] > df['mm20']) & (df['mm20'] > df['mm50']))
    ]
    choices = ['Achat']
    df['mm_decision'] = np.select(conditions, choices, default='Vente')
    
    # Gérer les NaN
    df['mm_decision'] = df['mm_decision'].where(
        df[['mm5', 'mm10', 'mm20', 'mm50']].notna().all(axis=1),
        'Attendre'
    )
    
    return df

def calculate_bollinger_bands(df, price_col='price', window=35, num_std=2):
    """Calcule les bandes de Bollinger avec vectorisation."""
    df['bollinger_central'] = df[price_col].rolling(window=window, min_periods=1).mean()
    rolling_std = df[price_col].rolling(window=window, min_periods=1).std()
    df['bollinger_superior'] = df['bollinger_central'] + (rolling_std * num_std)
    df['bollinger_inferior'] = df['bollinger_central'] - (rolling_std * num_std)
    
    # Décision vectorisée
    conditions = [
        df[price_col] <= df['bollinger_inferior'],
        df[price_col] >= df['bollinger_superior']
    ]
    choices = ['Achat', 'Vente']
    df['bollinger_decision'] = np.select(conditions, choices, default='Neutre')
    
    # Gérer les NaN
    df['bollinger_decision'] = df['bollinger_decision'].where(
        df[['bollinger_central', 'bollinger_inferior', 'bollinger_superior']].notna().all(axis=1),
        'Attendre'
    )
    
    return df

def calculate_macd(df, price_col='price', fast=12, slow=26, signal=9):
    """Calcule le MACD avec vectorisation."""
    df['macd_line'] = df[price_col].ewm(span=fast, adjust=False).mean() - df[price_col].ewm(span=slow, adjust=False).mean()
    df['signal_line'] = df['macd_line'].ewm(span=signal, adjust=False).mean()
    df['histogram'] = df['macd_line'] - df['signal_line']
    df['prev_histo'] = df['histogram'].shift(1)
    
    # Décision vectorisée
    conditions = [
        (df['prev_histo'] <= 0) & (df['histogram'] > 0),
        (df['prev_histo'] >= 0) & (df['histogram'] < 0),
        df['histogram'] > 0,
        df['histogram'] < 0
    ]
    choices = ['Achat (Fort)', 'Vente (Fort)', 'Achat', 'Vente']
    df['macd_decision'] = np.select(conditions, choices, default='Neutre')
    
    # Gérer les NaN
    df['macd_decision'] = df['macd_decision'].where(
        df[['histogram', 'prev_histo']].notna().all(axis=1),
        'Attendre'
    )
    
    return df

def calculate_rsi(df, price_col='price', period=20):
    """Calcule le RSI avec vectorisation."""
    delta = df[price_col].diff(1)
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.where(delta < 0, 0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi'] = 100 - (100 / (1 + rs))
    df['prev_rsi'] = df['rsi'].shift(1)
    
    # Décision vectorisée
    conditions = [
        (df['prev_rsi'] <= 30) & (df['rsi'] > 30),
        (df['prev_rsi'] >= 70) & (df['rsi'] < 70)
    ]
    choices = ['Achat', 'Vente']
    df['rsi_decision'] = np.select(conditions, choices, default='Neutre')
    
    # Gérer les NaN
    df['rsi_decision'] = df['rsi_decision'].where(
        df[['rsi', 'prev_rsi']].notna().all(axis=1),
        'Attendre'
    )
    
    return df

def calculate_stochastic(df, price_col='price', k_period=20, d_period=5):
    """Calcule le Stochastique avec vectorisation."""
    rolling_high = df[price_col].rolling(window=k_period, min_periods=1).max()
    rolling_low = df[price_col].rolling(window=k_period, min_periods=1).min()
    df['stochastic_k'] = 100 * ((df[price_col] - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan))
    df['stochastic_d'] = df['stochastic_k'].rolling(window=d_period, min_periods=1).mean()
    df['prev_k'] = df['stochastic_k'].shift(1)
    df['prev_d'] = df['stochastic_d'].shift(1)
    
    # Décision vectorisée
    conditions = [
        (df['prev_k'] <= df['prev_d']) & (df['stochastic_k'] > df['stochastic_d']) & (df['stochastic_d'] < 20),
        (df['prev_k'] >= df['prev_d']) & (df['stochastic_k'] < df['stochastic_d']) & (df['stochastic_d'] > 80)
    ]
    choices = ['Achat (Fort)', 'Vente (Fort)']
    df['stochastic_decision'] = np.select(conditions, choices, default='Neutre')
    
    # Gérer les NaN
    df['stochastic_decision'] = df['stochastic_decision'].where(
        df[['stochastic_k', 'stochastic_d', 'prev_k', 'prev_d']].notna().all(axis=1),
        'Attendre'
    )
    
    return df

def update_gsheet_with_technical_analysis(spreadsheet, symbol, df_technical):
    """
    Met à jour le Google Sheet avec les indicateurs techniques.
    Utilise batch update pour optimiser les performances.
    """
    try:
        worksheet = spreadsheet.worksheet(symbol)
        
        # Récupérer les dates existantes (colonne B)
        all_values = worksheet.get_all_values()
        if len(all_values) <= 1:
            logging.warning(f"  ⚠️ Feuille {symbol} vide ou sans données")
            return
        
        # Créer un mapping date -> row_index
        date_to_row = {}
        for i, row in enumerate(all_values[1:], start=2):  # Skip header, start at row 2
            if len(row) > 1 and row[1]:  # Colonne B = Date
                date_to_row[row[1]] = i
        
        if not date_to_row:
            logging.warning(f"  ⚠️ Aucune date trouvée dans {symbol}")
            return
        
        # Préparer les updates par batch
        updates = []
        
        for _, row_data in df_technical.iterrows():
            # Convertir la date au format DD/MM/YYYY
            try:
                trade_date = pd.to_datetime(row_data['trade_date'])
                date_str = trade_date.strftime('%d/%m/%Y')
            except:
                continue
            
            if date_str not in date_to_row:
                continue
            
            row_index = date_to_row[date_str]
            
            # Préparer les valeurs pour colonnes F à X
            tech_values = [
                row_data.get('mm5', ''),
                row_data.get('mm10', ''),
                row_data.get('mm20', ''),
                row_data.get('mm50', ''),
                row_data.get('mm_decision', ''),
                row_data.get('bollinger_central', ''),
                row_data.get('bollinger_inferior', ''),
                row_data.get('bollinger_superior', ''),
                row_data.get('bollinger_decision', ''),
                row_data.get('macd_line', ''),
                row_data.get('signal_line', ''),
                row_data.get('histogram', ''),
                row_data.get('macd_decision', ''),
                '',  # RS (calculé séparément si besoin)
                row_data.get('rsi', ''),
                row_data.get('rsi_decision', ''),
                row_data.get('stochastic_k', ''),
                row_data.get('stochastic_d', ''),
                row_data.get('stochastic_decision', ''),
            ]
            
            # Convertir NaN en chaîne vide et arrondir les nombres
            tech_values = [
                '' if pd.isna(v) else (round(float(v), 4) if isinstance(v, (int, float, np.number)) else str(v))
                for v in tech_values
            ]
            
            range_name = f'F{row_index}:X{row_index}'
            updates.append({'range': range_name, 'values': [tech_values]})
        
        # Effectuer toutes les mises à jour en batch (max 100 à la fois pour éviter rate limit)
        if updates:
            for i in range(0, len(updates), 100):
                batch = updates[i:i+100]
                worksheet.batch_update(batch, value_input_option='USER_ENTERED')
                if i + 100 < len(updates):
                    time.sleep(1)  # Pause entre les batchs
            
            logging.info(f"  ✅ {len(updates)} lignes mises à jour dans GSheet pour {symbol}")
        else:
            logging.info(f"  ℹ️ Aucune ligne à mettre à jour pour {symbol}")
        
    except gspread.exceptions.WorksheetNotFound:
        logging.warning(f"  ⚠️ Feuille '{symbol}' non trouvée")
    except Exception as e:
        logging.error(f"  ❌ Erreur mise à jour GSheet pour {symbol}: {e}")

def process_company_batch(conn, spreadsheet, company_id, company_symbol):
    """
    Traite l'analyse technique pour une société avec batch processing optimisé.
    """
    logging.info(f"--- Traitement : {company_symbol} ---")
    
    query = "SELECT id, trade_date, price FROM historical_data WHERE company_id = %s ORDER BY trade_date;"
    df = pd.read_sql(query, conn, params=(company_id,), index_col='id')
    
    if len(df) < 50:
        logging.warning(f"  -> Pas assez de données ({len(df)} lignes). Ignoré.")
        return 0

    # Calculer tous les indicateurs (vectorisé)
    df = calculate_moving_averages(df)
    df = calculate_bollinger_bands(df)
    df = calculate_macd(df)
    df = calculate_rsi(df)
    df = calculate_stochastic(df)
    
    # Préparer les données pour l'insertion batch
    df_to_update = df.drop(columns=['trade_date', 'price', 'prev_histo', 'prev_rsi', 'prev_k', 'prev_d']).reset_index()
    df_to_update.rename(columns={'id': 'historical_data_id'}, inplace=True)
    df_to_update = df_to_update.replace({np.nan: None})
    
    # Insertion batch dans PostgreSQL
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO technical_analysis (
            historical_data_id, mm5, mm10, mm20, mm50, mm_decision, 
            bollinger_central, bollinger_inferior, bollinger_superior, bollinger_decision, 
            macd_line, signal_line, histogram, macd_decision, 
            rsi, rsi_decision, stochastic_k, stochastic_d, stochastic_decision
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (historical_data_id) DO UPDATE SET
            mm5 = EXCLUDED.mm5, mm10 = EXCLUDED.mm10, mm20 = EXCLUDED.mm20, mm50 = EXCLUDED.mm50, 
            mm_decision = EXCLUDED.mm_decision,
            bollinger_central = EXCLUDED.bollinger_central, 
            bollinger_inferior = EXCLUDED.bollinger_inferior, 
            bollinger_superior = EXCLUDED.bollinger_superior, 
            bollinger_decision = EXCLUDED.bollinger_decision,
            macd_line = EXCLUDED.macd_line, signal_line = EXCLUDED.signal_line, 
            histogram = EXCLUDED.histogram, macd_decision = EXCLUDED.macd_decision,
            rsi = EXCLUDED.rsi, rsi_decision = EXCLUDED.rsi_decision, 
            stochastic_k = EXCLUDED.stochastic_k, stochastic_d = EXCLUDED.stochastic_d, 
            stochastic_decision = EXCLUDED.stochastic_decision;
    """
    
    # Préparer les valeurs pour le batch
    values = [
        (
            row['historical_data_id'], row.get('mm5'), row.get('mm10'), row.get('mm20'), 
            row.get('mm50'), row.get('mm_decision'),
            row.get('bollinger_central'), row.get('bollinger_inferior'), 
            row.get('bollinger_superior'), row.get('bollinger_decision'),
            row.get('macd_line'), row.get('signal_line'), row.get('histogram'), 
            row.get('macd_decision'),
            row.get('rsi'), row.get('rsi_decision'), row.get('stochastic_k'), 
            row.get('stochastic_d'), row.get('stochastic_decision')
        )
        for _, row in df_to_update.iterrows()
    ]
    
    # Exécution batch
    execute_batch(cur, insert_query, values, page_size=BATCH_SIZE)
    update_count = len(values)
    conn.commit()
    cur.close()
    
    logging.info(f"  -> PostgreSQL: {update_count} enregistrements mis à jour (batch)")
    
    # Mettre à jour Google Sheets
    df_for_gsheet = df.reset_index()
    update_gsheet_with_technical_analysis(spreadsheet, company_symbol, df_for_gsheet)
    
    return update_count

def delete_technical_sheets(spreadsheet):
    """Supprime toutes les feuilles se terminant par '_Technical'."""
    try:
        all_worksheets = spreadsheet.worksheets()
        deleted_count = 0
        
        for worksheet in all_worksheets:
            if worksheet.title.endswith('_Technical'):
                logging.info(f"  🗑️  Suppression de la feuille: {worksheet.title}")
                spreadsheet.del_worksheet(worksheet)
                deleted_count += 1
                time.sleep(0.5)  # Pause pour éviter rate limit
        
        if deleted_count > 0:
            logging.info(f"✅ {deleted_count} feuille(s) '_Technical' supprimée(s)")
        else:
            logging.info("ℹ️  Aucune feuille '_Technical' à supprimer")
            
    except Exception as e:
        logging.error(f"❌ Erreur lors de la suppression des feuilles: {e}")

def run_technical_analysis():
    """Fonction principale pour lancer l'analyse technique."""
    logging.info("="*80)
    logging.info("ÉTAPE 2 : ANALYSE TECHNIQUE (V6.0 - BATCH + GOOGLE SHEETS)")
    logging.info("="*80)
    
    conn = connect_to_db()
    gc = authenticate_gsheets()
    
    if not conn:
        logging.error("❌ Impossible de continuer sans connexion PostgreSQL")
        return
    
    spreadsheet = None
    if gc:
        try:
            spreadsheet = gc.open_by_key(SPREADSHEET_ID)
            logging.info(f"✅ Google Sheet ouvert: {spreadsheet.title}")
            
            # Supprimer les feuilles '_Technical'
            delete_technical_sheets(spreadsheet)
            
        except Exception as e:
            logging.error(f"❌ Impossible d'ouvrir le Google Sheet: {e}")
            logging.warning("⚠️ Continuation sans Google Sheets")
    else:
        logging.warning("⚠️ Google Sheets non disponible, utilisation PostgreSQL uniquement")

    try:
        cur = conn.cursor()
        cur.execute("SELECT id, symbol FROM companies;")
        companies = cur.fetchall()
        cur.close()

        logging.info(f"📊 {len(companies)} sociétés à analyser")
        
        start_time = time.time()
        total_updates = 0
        
        for company_id, company_symbol in companies:
            total_updates += process_company_batch(conn, spreadsheet, company_id, company_symbol)
            time.sleep(0.5)  # Pause légère pour éviter rate limit Google Sheets

        elapsed_time = time.time() - start_time
        
        logging.info(f"\n{'='*80}")
        logging.info(f"✅ Analyse technique terminée en {elapsed_time:.2f}s")
        logging.info(f"📊 Total: {total_updates} mises à jour")
        logging.info(f"⚡ Performance: {total_updates/elapsed_time:.1f} updates/s")
        logging.info(f"{'='*80}")

    except Exception as e:
        logging.error(f"❌ Erreur critique : {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_technical_analysis()
