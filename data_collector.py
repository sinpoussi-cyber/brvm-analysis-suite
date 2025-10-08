# ==============================================================================
# MODULE: DATA COLLECTOR (V3.0 - SYNC SUPABASE + GSHEET)
# ==============================================================================

import re
import time
import logging
import os
from io import BytesIO
from datetime import datetime
import pdfplumber
import requests
from bs4 import BeautifulSoup
import psycopg2
import urllib3
import gspread
from google.oauth2 import service_account
import json

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

def connect_to_db():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        logging.info("✅ Connexion à PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à PostgreSQL: {e}")
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

def get_company_ids(cur):
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur récupération IDs sociétés : {e}")
        return {}

def extract_date_from_filename(url):
    """Extrait une date YYYYMMDD pour le tri."""
    date_match = re.search(r'(\d{8})', url)
    if date_match:
        return date_match.group(1)
    return '19000101'

def get_all_boc_links():
    """Récupère TOUS les liens de BOC disponibles sur le site."""
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"🔍 Recherche de TOUS les bulletins sur : {url}")
    
    all_links = set()
    page = 0
    
    while True:
        try:
            page_url = f"{url}?page={page}" if page > 0 else url
            r = requests.get(page_url, verify=False, timeout=30)
            soup = BeautifulSoup(r.content, 'html.parser')
            
            links_found = 0
            for a in soup.find_all('a', href=True):
                href = a['href'].strip()
                if 'boc_' in href.lower() and href.endswith('.pdf'):
                    full_url = href if href.startswith('http') else "https://www.brvm.org" + href
                    if full_url not in all_links:
                        all_links.add(full_url)
                        links_found += 1
            
            if links_found == 0:
                logging.info(f"  Fin de pagination à la page {page}")
                break
            
            logging.info(f"  Page {page}: {links_found} nouveaux BOCs trouvés")
            page += 1
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"  Erreur sur la page {page}: {e}")
            break
    
    sorted_links = sorted(list(all_links), key=extract_date_from_filename, reverse=True)
    logging.info(f"✅ Total de {len(sorted_links)} BOCs trouvés sur le site")
    return sorted_links

def clean_and_convert_numeric(value):
    if value is None or value == '': return None
    cleaned_value = re.sub(r'\s+', '', str(value)).replace(',', '.')
    try: 
        return float(cleaned_value)
    except (ValueError, TypeError): 
        return None

def extract_data_from_pdf(pdf_url):
    """Extrait les données d'un PDF."""
    logging.info(f"  📄 Analyse du PDF : {os.path.basename(pdf_url)}")
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
                        if len(row) < 8: continue
                        vol, val = row[-8], row[-7]
                        cours = row[-6] if len(row) >= 6 else ""
                        symbole = row[1] if len(row) > 1 and row[1] and len(row[1]) <= 5 else row[0]
                        if re.search(r'\d', str(vol)) or re.search(r'\d', str(val)):
                            data.append({"Symbole": symbole, "Cours": cours, "Volume": vol, "Valeur": val})
    except Exception as e:
        logging.error(f"  ❌ Erreur extraction PDF {os.path.basename(pdf_url)}: {e}")
    return data

def check_date_exists_in_db(cur, trade_date):
    """Vérifie si la date existe déjà dans PostgreSQL."""
    cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
    return cur.fetchone() is not None

def check_date_exists_in_gsheet(worksheet, trade_date):
    """Vérifie si la date existe déjà dans Google Sheet."""
    try:
        date_str = trade_date.strftime('%d/%m/%Y')
        all_dates = worksheet.col_values(2)  # Colonne B = Date
        return date_str in all_dates
    except Exception as e:
        logging.error(f"  ❌ Erreur vérification date dans GSheet: {e}")
        return False

def insert_data_to_db(conn, cur, company_ids, symbol, trade_date, price, volume, value):
    """Insère les données dans PostgreSQL."""
    if symbol not in company_ids:
        return False
    
    company_id = company_ids[symbol]
    try:
        cur.execute("""
            INSERT INTO historical_data (company_id, trade_date, price, volume, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (company_id, trade_date) DO NOTHING;
        """, (company_id, trade_date, price, volume, value))
        return cur.rowcount > 0
    except Exception as e:
        logging.error(f"  ❌ Erreur insertion DB pour {symbol}: {e}")
        return False

def insert_data_to_gsheet(spreadsheet, symbol, trade_date, price, volume):
    """Insère les données dans Google Sheet."""
    try:
        worksheet = spreadsheet.worksheet(symbol)
        date_str = trade_date.strftime('%d/%m/%Y')
        
        # Vérifier si la date existe déjà
        if check_date_exists_in_gsheet(worksheet, trade_date):
            return False
        
        # Ajouter la ligne : [Symbol, Date, Price, Volume]
        new_row = [symbol, date_str, price, volume]
        worksheet.append_row(new_row, value_input_option='USER_ENTERED')
        return True
        
    except gspread.exceptions.WorksheetNotFound:
        logging.warning(f"  ⚠️ Feuille '{symbol}' non trouvée dans Google Sheets")
        return False
    except Exception as e:
        logging.error(f"  ❌ Erreur insertion GSheet pour {symbol}: {e}")
        return False

def run_data_collection():
    logging.info("="*80)
    logging.info("ÉTAPE 1 : COLLECTE DE DONNÉES (V3.0 - SYNC COMPLÈTE)")
    logging.info("="*80)
    
    conn = connect_to_db()
    gc = authenticate_gsheets()
    
    if not conn or not gc:
        logging.error("❌ Impossible de continuer sans connexion aux bases de données")
        return
    
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        logging.info(f"✅ Google Sheet ouvert: {spreadsheet.title}")
    except Exception as e:
        logging.error(f"❌ Impossible d'ouvrir le Google Sheet: {e}")
        if conn: conn.close()
        return
    
    try:
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
            logging.info(f"✅ {len(company_ids)} sociétés chargées depuis la DB")
        
        # Récupérer TOUS les BOCs du site
        all_boc_links = get_all_boc_links()
        
        total_new_db = 0
        total_new_gsheet = 0
        bocs_processed = 0
        bocs_skipped = 0
        
        for boc_url in all_boc_links:
            # Extraire la date du nom du fichier
            date_match = re.search(r'(\d{8})', boc_url)
            if not date_match:
                logging.warning(f"⚠️ Impossible d'extraire la date de: {os.path.basename(boc_url)}")
                continue
            
            date_yyyymmdd = date_match.group(1)
            try:
                trade_date = datetime.strptime(date_yyyymmdd, '%Y%m%d').date()
            except ValueError:
                logging.warning(f"⚠️ Date invalide: {date_yyyymmdd}")
                continue
            
            with conn.cursor() as cur:
                # Vérifier si cette date existe déjà dans PostgreSQL
                db_exists = check_date_exists_in_db(cur, trade_date)
            
            # Vérifier dans n'importe quelle feuille GSheet (on prend la première société)
            first_symbol = list(company_ids.keys())[0] if company_ids else None
            gsheet_exists = False
            if first_symbol:
                try:
                    worksheet = spreadsheet.worksheet(first_symbol)
                    gsheet_exists = check_date_exists_in_gsheet(worksheet, trade_date)
                except:
                    pass
            
            # Si existe dans les DEUX bases, on skip
            if db_exists and gsheet_exists:
                bocs_skipped += 1
                if bocs_skipped % 10 == 0:
                    logging.info(f"  ⏭️  {bocs_skipped} BOCs déjà présents (skippés)")
                continue
            
            # Sinon, on traite ce BOC
            logging.info(f"\n{'='*60}")
            logging.info(f"📊 Traitement du BOC du {trade_date}")
            logging.info(f"{'='*60}")
            
            rows = extract_data_from_pdf(boc_url)
            if not rows:
                logging.warning(f"  ⚠️ Aucune donnée extraite du PDF")
                continue
            
            new_records_db = 0
            new_records_gsheet = 0
            
            with conn.cursor() as cur:
                for rec in rows:
                    symbol = rec.get('Symbole', '').strip()
                    if symbol not in company_ids:
                        continue
                    
                    try:
                        price = clean_and_convert_numeric(rec.get('Cours'))
                        volume = int(clean_and_convert_numeric(rec.get('Volume')) or 0)
                        value = clean_and_convert_numeric(rec.get('Valeur'))
                        
                        # Insertion PostgreSQL
                        if not db_exists:
                            if insert_data_to_db(conn, cur, company_ids, symbol, trade_date, price, volume, value):
                                new_records_db += 1
                        
                        # Insertion Google Sheets
                        if not gsheet_exists:
                            if insert_data_to_gsheet(spreadsheet, symbol, trade_date, price, volume):
                                new_records_gsheet += 1
                        
                    except (ValueError, TypeError) as e:
                        logging.debug(f"  Données invalides pour {symbol}: {e}")
                        continue
            
            conn.commit()
            
            if new_records_db > 0 or new_records_gsheet > 0:
                logging.info(f"  ✅ {new_records_db} enregistrements → PostgreSQL")
                logging.info(f"  ✅ {new_records_gsheet} enregistrements → Google Sheets")
                total_new_db += new_records_db
                total_new_gsheet += new_records_gsheet
                bocs_processed += 1
            else:
                logging.info(f"  ℹ️ Données déjà présentes pour cette date")
                bocs_skipped += 1
            
            time.sleep(1)  # Pause pour éviter de surcharger l'API Google Sheets
        
        logging.info("\n" + "="*80)
        logging.info("📊 RÉSUMÉ DE LA COLLECTE")
        logging.info("="*80)
        logging.info(f"  BOCs traités: {bocs_processed}")
        logging.info(f"  BOCs skippés (déjà présents): {bocs_skipped}")
        logging.info(f"  Total nouveaux enregistrements PostgreSQL: {total_new_db}")
        logging.info(f"  Total nouveaux enregistrements Google Sheets: {total_new_gsheet}")
        logging.info("="*80)
        
    except Exception as e:
        logging.error(f"❌ Erreur critique: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
    
    logging.info("✅ Processus de collecte terminé.")

if __name__ == "__main__":
    run_data_collection()
