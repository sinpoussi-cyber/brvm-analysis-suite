# ==============================================================================
# MODULE: EXPORT TO GOOGLE SHEETS (V1.0)
# ==============================================================================
import psycopg2
import gspread
from google.oauth2 import service_account
import pandas as pd
import os
import json
import logging
from datetime import date

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

def authenticate_gsheets():
    try:
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google Sheets pour l'export réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur d'authentification Google Sheets : {e}")
        return None

def export_today_data():
    logging.info("="*60)
    logging.info("ÉTAPE SUPPLÉMENTAIRE : EXPORTATION VERS GOOGLE SHEETS")
    logging.info("="*60)
    
    conn = None
    gc = authenticate_gsheets()
    if not gc: return

    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        logging.info("✅ Connexion à PostgreSQL pour l'export réussie.")
        
        # Récupérer les données du jour
        today_str = date.today().strftime('%Y-%m-%d')
        query = f"""
        SELECT c.symbol, TO_CHAR(hd.trade_date, 'DD/MM/YYYY') as date, hd.price, hd.volume
        FROM historical_data hd
        JOIN companies c ON hd.company_id = c.id
        WHERE hd.trade_date = '{today_str}';
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            logging.warning("Aucune nouvelle donnée pour aujourd'hui à exporter vers Google Sheets.")
            return

        logging.info(f"Trouvé {len(df)} enregistrements pour aujourd'hui à exporter.")
        
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        for symbol, group in df.groupby('symbol'):
            try:
                worksheet = spreadsheet.worksheet(symbol)
                # Préparer les données pour l'ajout
                # Gspread attend une liste de listes
                rows_to_append = group[['symbol', 'date', 'price', 'volume']].values.tolist()
                worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                logging.info(f"  -> {len(rows_to_append)} ligne(s) exportée(s) vers la feuille '{symbol}'.")
            except gspread.exceptions.WorksheetNotFound:
                logging.warning(f"  -> Feuille '{symbol}' non trouvée dans Google Sheets. Ignoré.")
            except Exception as e:
                logging.error(f"  -> Erreur lors de l'export vers la feuille '{symbol}': {e}")
                
    except Exception as e:
        logging.error(f"❌ Erreur critique lors de l'export vers Google Sheets : {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    export_today_data()
