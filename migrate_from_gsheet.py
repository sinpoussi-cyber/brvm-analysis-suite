# ==============================================================================
# MODULE: MIGRATE FROM GOOGLE SHEETS TO POSTGRESQL (V1.2 - ROBUSTE)
# ==============================================================================

import gspread
import psycopg2
import pandas as pd
import os
import re # Importer le module des expressions régulières
from datetime import datetime
import logging
import time
import json
from google.oauth2 import service_account
import numpy as np

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Récupération des Secrets ---
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

def authenticate_gsheets():
    logging.info("Authentification Google Sheets...")
    try:
        if not GSPREAD_SERVICE_ACCOUNT_JSON:
            logging.error("❌ Secret GSPREAD_SERVICE_ACCOUNT introuvable.")
            return None
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google Sheets réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur d'authentification Google Sheets : {e}")
        return None

def connect_to_db():
    conn = None
    try:
        if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
            logging.error("❌ Un ou plusieurs secrets de base de données (DB_...) sont manquants.")
            return None
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        logging.info(f"✅ Connecté à la base de données PostgreSQL '{DB_NAME}'.")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur de connexion à la base de données PostgreSQL : {e}")
        return None

def clean_and_convert_to_numeric(value):
    """Nettoie une chaîne de caractères et la convertit en nombre."""
    if value is None or value == '':
        return None
    # Supprimer les espaces, remplacer la virgule par un point
    cleaned_value = re.sub(r'\s+', '', str(value)).replace(',', '.')
    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return None

def migrate_data():
    gc = authenticate_gsheets()
    if not gc: return

    db_conn = connect_to_db()
    if not db_conn: return

    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        logging.info(f"Ouverture du Google Sheet: {spreadsheet.title}")

        sheets_to_exclude = ["UNMATCHED", "Actions_BRVM", "ANALYSIS_MEMORY"]
        all_worksheets = spreadsheet.worksheets()
        
        cur = db_conn.cursor()

        for ws in all_worksheets:
            sheet_name = ws.title
            if sheet_name in sheets_to_exclude:
                logging.info(f"Feuille '{sheet_name}' ignorée.")
                continue

            logging.info(f"\n--- Migration des données pour la feuille: '{sheet_name}' ---")
            time.sleep(5)  # Pause pour respecter les limites de l'API GSheets
            data = ws.get_all_values()

            if not data or len(data) < 2:
                logging.warning(f"  -> La feuille '{sheet_name}' est vide ou n'a pas d'en-tête, ignorée.")
                continue

            headers = data[0]
            df = pd.DataFrame(data[1:], columns=headers)

            cur.execute("INSERT INTO companies (symbol, name) VALUES (%s, %s) ON CONFLICT (symbol) DO NOTHING RETURNING id;", (sheet_name, sheet_name))
            res = cur.fetchone()
            if res:
                company_id = res[0]
                logging.info(f"  -> Société '{sheet_name}' (ID: {company_id}) insérée.")
            else:
                cur.execute("SELECT id FROM companies WHERE symbol = %s;", (sheet_name,))
                company_id = cur.fetchone()[0]
                logging.info(f"  -> Société '{sheet_name}' (ID: {company_id}) déjà existante.")


            if 'Date' in df.columns and 'Cours (F CFA)' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
                
                # Appliquer la fonction de nettoyage robuste
                for col in df.columns:
                    if col not in ['Date', 'Symbole', 'MMdecision', 'Boldecision', 'MACDdecision', 'RSIdecision', 'Stocdecision']:
                        df[col] = df[col].apply(clean_and_convert_to_numeric)
                
                df.replace({pd.NaT: None, np.nan: None}, inplace=True)

                for index, row in df.iterrows():
                    if row.get('Date') is None: continue

                    cur.execute("""
                        INSERT INTO historical_data (company_id, trade_date, price, volume, value)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (company_id, trade_date) DO UPDATE SET price = EXCLUDED.price, volume = EXCLUDED.volume, value = EXCLUDED.value
                        RETURNING id;
                    """, (company_id, row.get('Date'), row.get('Cours (F CFA)'), row.get('Volume échangé'), row.get('Valeurs échangées (F CFA)')))
                    historical_data_id = cur.fetchone()[0]

                    cur.execute("""
                        INSERT INTO technical_analysis (historical_data_id, mm5, mm10, mm20, mm50, mm_decision, bollinger_central, bollinger_inferior, bollinger_superior, bollinger_decision, macd_line, signal_line, histogram, macd_decision, rsi, rsi_decision, stochastic_k, stochastic_d, stochastic_decision)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (historical_data_id) DO NOTHING;
                    """, (
                        historical_data_id, row.get('MM5'), row.get('MM10'), row.get('MM20'), row.get('MM50'), row.get('MMdecision'),
                        row.get('Bande_centrale'), row.get('Bande_Inferieure'), row.get('Bande_Supérieure'), row.get('Boldecision'),
                        row.get('Ligne MACD'), row.get('Ligne de signal'), row.get('Histogramme'), row.get('MACDdecision'),
                        row.get('RSI'), row.get('RSIdecision'), row.get('%K'), row.get('%D'), row.get('Stocdecision')
                    ))
                logging.info(f"  -> {len(df)} lignes de données historiques/techniques traitées.")

        logging.info("\n--- Migration des analyses fondamentales ---")
        try:
            analysis_ws = spreadsheet.worksheet("ANALYSIS_MEMORY")
            time.sleep(5)
            memory_data = analysis_ws.get_all_records()
            for record in memory_data:
                url, symbol, summary = record.get('URL'), record.get('Symbol'), record.get('Analysis_Summary')
                if not all([url, symbol, summary]): continue
                
                cur.execute("SELECT id FROM companies WHERE symbol = %s;", (symbol,))
                company_id_res = cur.fetchone()
                if company_id_res:
                    cur.execute("""
                        INSERT INTO fundamental_analysis (company_id, report_url, analysis_summary)
                        VALUES (%s, %s, %s) ON CONFLICT (report_url) DO NOTHING;
                    """, (company_id_res[0], url, summary))
            logging.info(f"  -> {len(memory_data)} lignes d'analyses fondamentales traitées.")
        except gspread.exceptions.WorksheetNotFound:
            logging.warning("  -> Feuille 'ANALYSIS_MEMORY' non trouvée.")

        db_conn.commit()
        logging.info("✅ Migration terminée avec succès. Transaction validée.")

    except Exception as e:
        logging.error(f"❌ Erreur critique durant la migration : {e}", exc_info=True)
        db_conn.rollback()
    finally:
        if db_conn:
            db_conn.close()

if __name__ == "__main__":
    logging.info("🚀 Démarrage de la migration des données de Google Sheets vers PostgreSQL.")
    migrate_data()
    logging.info("🏁 Fin de la migration.")
