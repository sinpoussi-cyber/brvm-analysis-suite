# ==============================================================================
# MODULE: DATA COLLECTOR (V3.2 - ORDRE CHRONOLOGIQUE + COLONNE E)
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
    """
    Récupère TOUS les liens de BOC disponibles sur le site BRVM.
    """
    base_url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"🔍 Recherche de TOUS les bulletins sur : {base_url}")
    
    all_links = set()
    page = 0
    consecutive_empty = 0
    
    while consecutive_empty < 3:
        try:
            page_url = f"{base_url}?page={page}" if page > 0 else base_url
            logging.info(f"  Exploration de la page {page}...")
            
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
                consecutive_empty += 1
                logging.info(f"  Page {page} : 0 nouveau BOC (vide #{consecutive_empty})")
            else:
                consecutive_empty = 0
                logging.info(f"  Page {page} : {links_found} nouveau(x) BOC(s) trouvé(s)")
            
            page += 1
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"  Erreur sur la page {page}: {e}")
            consecutive_empty += 1
    
    # Trier du PLUS ANCIEN au PLUS RÉCENT (ordre chronologique)
    sorted_links = sorted(list(all_links), key=extract_date_from_filename, reverse=False)
    logging.info(f"✅ Total de {len(sorted_links)} BOCs trouvés (triés du plus ancien au plus récent)")
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
                            data.append({
                                "Symbole": symbole, 
                                "Cours": cours, 
                                "Volume": vol, 
                                "Valeur": val
                            })
    except Exception as e:
        logging.error(f"  ❌ Erreur extraction PDF : {e}")
    return data

def check_date_exists_in_db(cur, trade_date):
    """Vérifie si la date existe déjà dans PostgreSQL."""
    cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
    return cur.fetchone() is not None

def get_last_date_in_gsheet(worksheet):
    """Récupère la dernière date dans Google Sheet pour savoir où insérer."""
    try:
        all_dates = worksheet.col_values(2)[1:]  # Colonne B, skip header
        if not all_dates:
            return None
        # Dernière date dans la feuille
        last_date_str = all_dates[-1]
        return datetime.strptime(last_date_str, '%d/%m/%Y').date()
    except Exception as e:
        logging.error(f"  ❌ Erreur lecture dernière date GSheet: {e}")
        return None

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

def insert_data_to_gsheet(spreadsheet, symbol, trade_date, price, volume, value):
    """
    Insère les données dans Google Sheet EN BAS (ordre chronologique).
    Colonnes: A=Symbol, B=Date, C=Price, D=Volume, E=Valeur (FCFA)
    """
    try:
        worksheet = spreadsheet.worksheet(symbol)
        date_str = trade_date.strftime('%d/%m/%Y')
        
        # Vérifier si la date existe déjà
        all_dates = worksheet.col_values(2)[1:]  # Colonne B, skip header
        if date_str in all_dates:
            return False
        
        # Ajouter EN BAS (append_row ajoute toujours en bas)
        new_row = [symbol, date_str, price, volume, value]
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
    logging.info("ÉTAPE 1 : COLLECTE DE DONNÉES (V3.2 - ORDRE CHRONOLOGIQUE)")
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
        except Exception as e:
            logging.error(f"❌ Impossible d'ouvrir le Google Sheet: {e}")
            logging.warning("⚠️ Continuation sans Google Sheets")
    
    try:
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
            logging.info(f"✅ {len(company_ids)} sociétés chargées depuis la DB")
        
        # Récupérer TOUS les BOCs triés du PLUS ANCIEN au PLUS RÉCENT
        all_boc_links = get_all_boc_links()
        
        total_new_db = 0
        total_new_gsheet = 0
        bocs_processed = 0
        bocs_skipped = 0
        
        logging.info(f"\n{'='*80}")
        logging.info(f"📊 Traitement de {len(all_boc_links)} BOCs (ordre chronologique)")
        logging.info(f"{'='*80}\n")
        
        for idx, boc_url in enumerate(all_boc_links, 1):
            # Extraire la date
            date_match = re.search(r'(\d{8})', boc_url)
            if not date_match:
                logging.warning(f"⚠️ [{idx}/{len(all_boc_links)}] Impossible d'extraire la date")
                continue
            
            date_yyyymmdd = date_match.group(1)
            try:
                trade_date = datetime.strptime(date_yyyymmdd, '%Y%m%d').date()
            except ValueError:
                logging.warning(f"⚠️ [{idx}/{len(all_boc_links)}] Date invalide: {date_yyyymmdd}")
                continue
            
            with conn.cursor() as cur:
                db_exists = check_date_exists_in_db(cur, trade_date)
            
            # Vérifier dans GSheet
            gsheet_exists = False
            if spreadsheet:
                first_symbol = list(company_ids.keys())[0] if company_ids else None
                if first_symbol:
                    try:
                        worksheet = spreadsheet.worksheet(first_symbol)
                        last_date = get_last_date_in_gsheet(worksheet)
                        if last_date and trade_date <= last_date:
                            gsheet_exists = True
                    except:
                        pass
            
            # Si existe dans les DEUX, skip
            if db_exists and gsheet_exists:
                bocs_skipped += 1
                if bocs_skipped % 50 == 0:
                    logging.info(f"  ⏭️  [{idx}/{len(all_boc_links)}] {bocs_skipped} BOCs déjà présents")
                continue
            
            # Traiter ce BOC
            logging.info(f"\n{'─'*80}")
            logging.info(f"📊 [{idx}/{len(all_boc_links)}] BOC du {trade_date} - {os.path.basename(boc_url)}")
            logging.info(f"{'─'*80}")
            
            rows = extract_data_from_pdf(boc_url)
            if not rows:
                logging.warning(f"  ⚠️ Aucune donnée extraite")
                bocs_skipped += 1
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
                        
                        # PostgreSQL
                        if not db_exists:
                            if insert_data_to_db(conn, cur, company_ids, symbol, trade_date, price, volume, value):
                                new_records_db += 1
                        
                        # Google Sheets (EN BAS = ordre chronologique)
                        if spreadsheet and not gsheet_exists:
                            if insert_data_to_gsheet(spreadsheet, symbol, trade_date, price, volume, value):
                                new_records_gsheet += 1
                        
                    except (ValueError, TypeError):
                        continue
            
            conn.commit()
            
            if new_records_db > 0 or new_records_gsheet > 0:
                logging.info(f"  ✅ PostgreSQL: {new_records_db} | Google Sheets: {new_records_gsheet}")
                total_new_db += new_records_db
                total_new_gsheet += new_records_gsheet
                bocs_processed += 1
            else:
                bocs_skipped += 1
            
            time.sleep(0.5)
        
        logging.info("\n" + "="*80)
        logging.info("📊 RÉSUMÉ DE LA COLLECTE (ORDRE CHRONOLOGIQUE)")
        logging.info("="*80)
        logging.info(f"  Total BOCs                  : {len(all_boc_links)}")
        logging.info(f"  BOCs traités (nouveaux)     : {bocs_processed}")
        logging.info(f"  BOCs skippés (déjà présents): {bocs_skipped}")
        logging.info(f"  Nouveaux enregistrements DB : {total_new_db}")
        logging.info(f"  Nouveaux enregistrements GS : {total_new_gsheet}")
        logging.info("="*80)
        
    except Exception as e:
        logging.error(f"❌ Erreur critique: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
    
    logging.info("✅ Collecte terminée (ordre chronologique respecté).")

if __name__ == "__main__":
    run_data_collection()
