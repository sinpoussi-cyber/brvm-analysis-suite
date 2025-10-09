# ==============================================================================
# MODULE: DATA COLLECTOR (V3.1 - ÉCRITURE SIMULTANÉE DB + GOOGLE SHEETS)
# ==============================================================================

import re
import time
import unicodedata
import logging
import os
import json
from io import BytesIO
from datetime import datetime

import pdfplumber
import requests
from bs4 import BeautifulSoup
import psycopg2
import urllib3
import gspread
from google.oauth2 import service_account

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# --- Connexion PostgreSQL ---
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD, 
            host=DB_HOST, 
            port=DB_PORT
        )
        logging.info("✅ Connexion à la base de données PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à la DB: {e}")
        return None

# --- Authentification Google Sheets ---
def authenticate_gsheets():
    try:
        creds_json = GSPREAD_SERVICE_ACCOUNT_JSON
        
        if not creds_json:
            logging.warning("⚠️  GSPREAD_SERVICE_ACCOUNT non défini, Google Sheets sera ignoré")
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
    except json.JSONDecodeError as e:
        logging.error(f"❌ Erreur de parsing JSON : {e}")
        return None
    except Exception as e:
        logging.error(f"❌ Erreur d'authentification Google Sheets : {e}")
        return None

# --- Récupération des IDs des sociétés ---
def get_company_ids(cur):
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur lors de la récupération des IDs des sociétés : {e}")
        return {}

# --- Extraction de date depuis nom de fichier ---
def extract_date_from_filename_for_sorting(url):
    date_match = re.search(r'(\d{8})', url)
    if date_match:
        return date_match.group(1)
    return '19000101'

# --- Récupération des liens BOC ---
def get_boc_links():
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"Recherche de bulletins sur : {url}")
    r = requests.get(url, verify=False, timeout=30)
    soup = BeautifulSoup(r.content, 'html.parser')
    links = set()
    
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if 'boc_' in href.lower() and href.endswith('.pdf'):
            full_url = href if href.startswith('http') else "https://www.brvm.org" + href
            links.add(full_url)
    
    if not links:
        logging.warning("Aucun lien de bulletin (BOC) trouvé sur la page principale.")

    sorted_links = sorted(list(links), key=extract_date_from_filename_for_sorting, reverse=True)
    return sorted_links[:15]

# --- Nettoyage et conversion numérique ---
def clean_and_convert_numeric(value):
    if value is None or value == '': 
        return None
    cleaned_value = re.sub(r'\s+', '', str(value)).replace(',', '.')
    try: 
        return float(cleaned_value)
    except (ValueError, TypeError): 
        return None

# --- Extraction des données depuis PDF ---
def extract_data_from_pdf(pdf_url):
    logging.info(f"Analyse du PDF : {pdf_url}")
    data = []
    try:
        r = requests.get(pdf_url, verify=False, timeout=30)
        pdf_file = BytesIO(r.content)
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables() or []
                for table in tables:
                    for row in table:
                        row = [(cell.strip() if cell else "") for cell in row]
                        if len(row) < 8: 
                            continue
                        vol, val = row[-8], row[-7]
                        cours = row[-6] if len(row) >= 6 else ""
                        symbole = row[1] if len(row) > 1 and row[1] and len(row[1]) <= 5 else row[0]
                        if re.search(r'\d', str(vol)) or re.search(r'\d', str(val)):
                            data.append({
                                "Symbole": symbole, 
                                "Cours": cours, 
                                "Volume": vol, 
                                "Valeur": val
                            })
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données du PDF {pdf_url}: {e}")
    return data

# --- Écriture dans Google Sheets ---
def write_to_gsheet(gc, symbol, trade_date, price, volume):
    """Écrit une ligne dans Google Sheets pour un symbole donné."""
    if not gc or not SPREADSHEET_ID:
        return False
    
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        try:
            worksheet = spreadsheet.worksheet(symbol)
        except gspread.exceptions.WorksheetNotFound:
            # Créer la feuille si elle n'existe pas
            worksheet = spreadsheet.add_worksheet(title=symbol, rows=1000, cols=10)
            worksheet.update([['Symbol', 'Date', 'Price', 'Volume']])
            logging.info(f"  📄 Feuille '{symbol}' créée dans Google Sheets")
        
        # Vérifier si la date existe déjà
        existing_data = worksheet.get_all_values()
        date_str = trade_date.strftime('%d/%m/%Y')
        
        for row in existing_data[1:]:  # Ignorer l'en-tête
            if len(row) > 1 and row[1] == date_str:
                return True  # Déjà présent, pas besoin d'ajouter
        
        # Ajouter la nouvelle ligne
        worksheet.append_row([
            symbol,
            date_str,
            price if price else '',
            volume if volume else ''
        ], value_input_option='USER_ENTERED')
        
        return True
    except Exception as e:
        logging.error(f"  ❌ Erreur Google Sheets pour {symbol}: {e}")
        return False

# --- Fonction principale de collecte ---
def run_data_collection():
    logging.info("="*60)
    logging.info("ÉTAPE 1 : DÉMARRAGE DE LA COLLECTE DE DONNÉES (V3.0)")
    logging.info("ÉCRITURE SIMULTANÉE : PostgreSQL + Google Sheets")
    logging.info("="*60)
    
    conn = connect_to_db()
    if not conn: 
        return
    
    gc = authenticate_gsheets()
    if not gc:
        logging.warning("⚠️  Google Sheets non disponible, seule PostgreSQL sera utilisée")
    
    try:
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
        
        boc_links = get_boc_links()
        logging.info(f"{len(boc_links)} BOCs récents trouvés sur le site.")
        
        total_new_records_db = 0
        total_new_records_gsheet = 0
        
        for boc in boc_links:
            date_match = re.search(r'(\d{8})', boc)
            if not date_match: 
                continue
            date_yyyymmdd = date_match.group(1)

            try:
                trade_date = datetime.strptime(date_yyyymmdd, '%Y%m%d').date()
            except ValueError:
                continue

            # Vérifier si les données existent déjà en DB
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
                if cur.fetchone():
                    logging.info(f"Les données pour la date {trade_date} existent déjà. Ignoré.")
                    continue
            
            logging.info(f"📅 Traitement des données pour la date {trade_date}...")
            rows = extract_data_from_pdf(boc)
            if not rows: 
                continue

            new_records_for_this_date_db = 0
            new_records_for_this_date_gsheet = 0
            
            with conn.cursor() as cur:
                for rec in rows:
                    symbol = rec.get('Symbole', '').strip()
                    if symbol in company_ids:
                        company_id = company_ids[symbol]
                        try:
                            price = clean_and_convert_numeric(rec.get('Cours'))
                            volume = int(clean_and_convert_numeric(rec.get('Volume')) or 0)
                            value = clean_and_convert_numeric(rec.get('Valeur'))
                            
                            # 1️⃣ ÉCRITURE DANS POSTGRESQL
                            cur.execute("""
                                INSERT INTO historical_data (company_id, trade_date, price, volume, value)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (company_id, trade_date) DO NOTHING;
                            """, (company_id, trade_date, price, volume, value))
                            
                            if cur.rowcount > 0:
                                new_records_for_this_date_db += 1
                                
                                # 2️⃣ ÉCRITURE DANS GOOGLE SHEETS (simultanée)
                                if gc:
                                    if write_to_gsheet(gc, symbol, trade_date, price, volume):
                                        new_records_for_this_date_gsheet += 1
                                
                        except (ValueError, TypeError):
                            pass  # Ignorer les données invalides

            if new_records_for_this_date_db > 0 or new_records_for_this_date_gsheet > 0:
                logging.info(f"  ✅ {trade_date}:")
                logging.info(f"     • PostgreSQL: {new_records_for_this_date_db} enregistrements")
                if gc:
                    logging.info(f"     • Google Sheets: {new_records_for_this_date_gsheet} enregistrements")
                total_new_records_db += new_records_for_this_date_db
                total_new_records_gsheet += new_records_for_this_date_gsheet
            else:
                logging.info(f"  -> Aucune nouvelle donnée pour le {trade_date}")

            conn.commit()

        logging.info("\n" + "="*60)
        logging.info(f"✅ COLLECTE TERMINÉE")
        logging.info(f"   📊 PostgreSQL: {total_new_records_db} nouveaux enregistrements")
        if gc:
            logging.info(f"   📊 Google Sheets: {total_new_records_gsheet} nouveaux enregistrements")
        logging.info("="*60)

    except Exception as e:
        logging.error(f"❌ Erreur critique dans la collecte de données : {e}", exc_info=True)
        if conn: 
            conn.rollback()
    finally:
        if conn: 
            conn.close()
    
    logging.info("Processus de collecte de données terminé.")

if __name__ == "__main__":
    run_data_collection()
