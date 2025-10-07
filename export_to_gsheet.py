# ==============================================================================
# MODULE: EXPORT TO GOOGLE SHEETS (V1.1 - VERSION CORRIGÉE)
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
    """Authentifie et retourne un client gspread."""
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
    """Exporte les données du jour vers Google Sheets."""
    logging.info("=" * 60)
    logging.info("ÉTAPE SUPPLÉMENTAIRE : EXPORTATION VERS GOOGLE SHEETS")
    logging.info("=" * 60)
    
    # Validation des secrets
    if not GSPREAD_SERVICE_ACCOUNT_JSON or not SPREADSHEET_ID:
        logging.warning("⚠️ Secrets Google Sheets manquants (GSPREAD_SERVICE_ACCOUNT ou SPREADSHEET_ID). Export ignoré.")
        return
    
    conn = None
    gc = authenticate_gsheets()
    
    if not gc:
        return
    
    try:
        # Connexion à PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("✅ Connexion à PostgreSQL pour l'export réussie.")
        
        # Récupérer les données du jour TRIÉES par date (plus ancien en haut)
        today_str = date.today().strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            c.symbol, 
            TO_CHAR(hd.trade_date, 'DD/MM/YYYY') as date, 
            hd.price, 
            hd.volume,
            hd.value,
            hd.trade_date as sort_date
        FROM historical_data hd
        JOIN companies c ON hd.company_id = c.id
        WHERE hd.trade_date = '{today_str}'
        ORDER BY c.symbol, hd.trade_date ASC;
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            logging.warning(f"⚠️ Aucune nouvelle donnée pour aujourd'hui ({today_str}) à exporter vers Google Sheets.")
            return
        
        logging.info(f"📊 Trouvé {len(df)} enregistrements pour aujourd'hui à exporter.")
        
        # Ouvrir le spreadsheet
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        # Grouper par symbole et exporter dans chaque feuille
        exported_count = 0
        errors_count = 0
        
        for symbol, group in df.groupby('symbol'):
            try:
                # Essayer d'accéder à la feuille existante
                try:
                    worksheet = spreadsheet.worksheet(symbol)
                except gspread.exceptions.WorksheetNotFound:
                    # Si la feuille n'existe pas, la créer
                    logging.info(f"  📄 Création de la feuille '{symbol}'...")
                    worksheet = spreadsheet.add_worksheet(title=symbol, rows=1000, cols=10)
                    # Ajouter les en-têtes
                    worksheet.append_row(['Symbole', 'Date', 'Prix', 'Volume', 'Valeur'], value_input_option='USER_ENTERED')
                
                # Préparer les données pour l'ajout (sans la colonne sort_date)
                rows_to_append = group[['symbol', 'date', 'price', 'volume', 'value']].values.tolist()
                
                # Ajouter les lignes (elles seront ajoutées en bas automatiquement)
                # Puisque nos données sont triées par date ASC dans la requête SQL,
                # l'historique complet sera trié du plus ancien (haut) au plus récent (bas)
                worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                
                logging.info(f"  ✅ {len(rows_to_append)} ligne(s) exportée(s) vers la feuille '{symbol}'.")
                exported_count += len(rows_to_append)# Préparer les données pour l'ajout
                rows_to_append = group[['symbol', 'date', 'price', 'volume', 'value']].values.tolist()
                
                # Ajouter les lignes
                worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                
                logging.info(f"  ✅ {len(rows_to_append)} ligne(s) exportée(s) vers la feuille '{symbol}'.")
                exported_count += len(rows_to_append)
            
            except Exception as e:
                logging.error(f"  ❌ Erreur lors de l'export vers la feuille '{symbol}': {e}")
                errors_count += 1
        
        # Résumé
        logging.info("\n" + "=" * 60)
        logging.info("📊 RÉSUMÉ DE L'EXPORT GOOGLE SHEETS")
        logging.info("=" * 60)
        logging.info(f"   • Lignes exportées : {exported_count}")
        logging.info(f"   • Erreurs : {errors_count}")
        logging.info("=" * 60)
        logging.info("✅ Export vers Google Sheets terminé")
    
    except Exception as e:
        logging.error(f"❌ Erreur critique lors de l'export vers Google Sheets : {e}", exc_info=True)
    
    finally:
        if conn and not conn.closed:
            conn.close()

if __name__ == "__main__":
    export_today_data()
