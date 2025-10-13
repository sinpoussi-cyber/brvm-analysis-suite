# ==============================================================================
# MODULE: DATA COLLECTOR V5.0 - OPTIMISÉ BATCH PROCESSING
# ==============================================================================

import re
import time
import logging
import os
import json
from io import BytesIO
from datetime import datetime
from collections import defaultdict

import pdfplumber
import requests
from bs4 import BeautifulSoup
import psycopg2
import urllib3
import gspread
from google.oauth2 import service_account

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# ═══════════════════════════════════════════════════════════
# RATE LIMITING GOOGLE SHEETS (CRITIQUE)
# ═══════════════════════════════════════════════════════════
MAX_REQUESTS_PER_MINUTE = 50  # Sécurité sous la limite de 60
request_timestamps = []

def rate_limit_gsheet():
    """Gestion stricte du rate limiting Google Sheets"""
    global request_timestamps
    now = time.time()
    
    # Garder seulement les requêtes des 60 dernières secondes
    request_timestamps = [t for t in request_timestamps if now - t < 60]
    
    if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
        sleep_time = 60 - (now - request_timestamps[0]) + 2  # +2s de sécurité
        logging.warning(f"⏸️  Rate limit atteint ({len(request_timestamps)} req). Pause {sleep_time:.1f}s")
        time.sleep(sleep_time)
        request_timestamps = []
    
    request_timestamps.append(time.time())

# --- Connexion PostgreSQL ---
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, 
            host=DB_HOST, port=DB_PORT
        )
        logging.info("✅ Connexion PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur connexion DB: {e}")
        return None

# --- Authentification Google Sheets ---
def authenticate_gsheets():
    try:
        if not GSPREAD_SERVICE_ACCOUNT_JSON:
            logging.warning("⚠️  GSPREAD_SERVICE_ACCOUNT non défini")
            return None
        
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google Sheets réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur authentification Google Sheets: {e}")
        return None

# --- Récupération des IDs sociétés ---
def get_company_ids(cur):
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur récupération IDs sociétés: {e}")
        return {}

# --- Extraction date depuis URL ---
def extract_date_from_url(url):
    date_match = re.search(r'boc_(\d{8})', url)
    if date_match:
        return date_match.group(1)
    return None

# --- Récupération TOUS les BOCs ---
def get_all_boc_links():
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"🔍 Recherche de TOUS les BOCs sur : {url}")
    
    try:
        r = requests.get(url, verify=False, timeout=30)
        soup = BeautifulSoup(r.content, 'html.parser')
        links = set()
        
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if 'boc_' in href.lower() and href.endswith('.pdf'):
                full_url = href if href.startswith('http') else "https://www.brvm.org" + href
                links.add(full_url)
        
        if not links:
            logging.warning("⚠️  Aucun BOC trouvé sur la page")
            return []
        
        sorted_links = sorted(list(links), key=lambda x: extract_date_from_url(x) or '19000101')
        logging.info(f"✅ {len(sorted_links)} BOC(s) trouvé(s)")
        return sorted_links
    
    except Exception as e:
        logging.error(f"❌ Erreur récupération BOCs: {e}")
        return []

# --- Vérification date dans DB ---
def date_exists_in_db(conn, trade_date):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
            return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"❌ Erreur vérification date DB: {e}")
        return False

# ═══════════════════════════════════════════════════════════
# NOUVELLE FONCTION : CHARGEMENT BATCH DES DATES EXISTANTES
# ═══════════════════════════════════════════════════════════
def load_existing_dates_batch(gc, spreadsheet, company_symbols):
    """
    Charge TOUTES les dates existantes en BATCH (optimisé)
    Retourne : {symbol: set(dates)}
    """
    logging.info("📂 Chargement des dates existantes (mode BATCH)...")
    existing_dates = defaultdict(set)
    
    if not gc or not spreadsheet:
        return existing_dates
    
    try:
        # Récupérer la liste des feuilles existantes (1 seule requête)
        rate_limit_gsheet()
        all_worksheets = spreadsheet.worksheets()
        worksheet_titles = {ws.title: ws for ws in all_worksheets}
        
        # Pour chaque société
        for symbol in company_symbols:
            if symbol not in worksheet_titles:
                continue
            
            try:
                worksheet = worksheet_titles[symbol]
                
                # Charger la colonne B (dates) en une seule requête
                rate_limit_gsheet()
                dates_column = worksheet.col_values(2)  # Colonne B
                
                # Enlever l'en-tête et stocker dans un set
                if len(dates_column) > 1:
                    existing_dates[symbol] = set(dates_column[1:])
                
            except Exception as e:
                logging.warning(f"   ⚠️  Erreur lecture {symbol}: {e}")
                continue
        
        total_dates = sum(len(dates) for dates in existing_dates.values())
        logging.info(f"   ✅ {len(existing_dates)} feuilles | {total_dates} dates chargées")
        return existing_dates
    
    except Exception as e:
        logging.error(f"❌ Erreur chargement batch: {e}")
        return defaultdict(set)

# --- Nettoyage valeurs numériques ---
def clean_and_convert_numeric(value):
    if value is None or value == '': 
        return None
    cleaned_value = re.sub(r'\s+', '', str(value)).replace(',', '.')
    try: 
        return float(cleaned_value)
    except (ValueError, TypeError): 
        return None

# --- Extraction données depuis PDF ---
def extract_data_from_pdf(pdf_url):
    logging.info(f"   📄 Analyse du PDF...")
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
                        
                        vol = row[-8]
                        val = row[-7]
                        cours = row[-6] if len(row) >= 6 else ""
                        symbole = row[1] if len(row) > 1 and row[1] and len(row[1]) <= 5 else row[0]
                        
                        if re.search(r'\d', str(vol)) or re.search(r'\d', str(val)):
                            data.append({
                                "Symbole": symbole, 
                                "Cours": cours, 
                                "Volume": vol, 
                                "Valeur": val
                            })
        
        logging.info(f"   ✓ {len(data)} ligne(s) extraite(s)")
        return data
    
    except Exception as e:
        logging.error(f"❌ Erreur extraction PDF: {e}")
        return []

# --- Insertion dans DB ---
def insert_into_db(conn, company_ids, symbol, trade_date, price, volume, value):
    if symbol not in company_ids:
        return False
    
    company_id = company_ids[symbol]
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO historical_data (company_id, trade_date, price, volume, value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (company_id, trade_date) DO NOTHING;
            """, (company_id, trade_date, price, volume, value))
            
            conn.commit()
            return cur.rowcount > 0
    
    except Exception as e:
        logging.error(f"❌ Erreur insertion DB pour {symbol}: {e}")
        conn.rollback()
        return False

# ═══════════════════════════════════════════════════════════
# NOUVELLE FONCTION : BATCH UPDATE GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════
def batch_update_gsheets(gc, spreadsheet, updates_by_symbol):
    """
    Met à jour toutes les feuilles en BATCH
    updates_by_symbol = {symbol: [[date, prix, volume], ...]}
    """
    if not updates_by_symbol:
        return 0
    
    logging.info(f"📤 Mise à jour batch Google Sheets ({len(updates_by_symbol)} feuilles)...")
    total_updated = 0
    
    for symbol, rows_to_add in updates_by_symbol.items():
        if not rows_to_add:
            continue
        
        try:
            # Ouvrir la feuille
            rate_limit_gsheet()
            try:
                worksheet = spreadsheet.worksheet(symbol)
            except gspread.exceptions.WorksheetNotFound:
                logging.warning(f"   ⚠️  Feuille '{symbol}' non trouvée, création...")
                rate_limit_gsheet()
                worksheet = spreadsheet.add_worksheet(title=symbol, rows=1000, cols=10)
                rate_limit_gsheet()
                worksheet.append_row(['Symbole', 'Date', 'Cours', 'Volume'], value_input_option='USER_ENTERED')
            
            # Trier chronologiquement
            rows_to_add.sort(key=lambda x: datetime.strptime(x[1], '%d/%m/%Y'))
            
            # UN SEUL append_rows pour toute la feuille
            rate_limit_gsheet()
            worksheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
            
            total_updated += len(rows_to_add)
            logging.info(f"   ✅ {symbol}: {len(rows_to_add)} lignes")
        
        except Exception as e:
            logging.error(f"   ❌ Erreur {symbol}: {e}")
            continue
    
    return total_updated

# --- Nettoyage des feuilles "_Technical" ---
def cleanup_technical_sheets(gc, spreadsheet):
    try:
        rate_limit_gsheet()
        worksheets = spreadsheet.worksheets()
        deleted_count = 0
        
        for ws in worksheets:
            if ws.title.endswith('_Technical'):
                logging.info(f"🗑️  Suppression de la feuille: {ws.title}")
                rate_limit_gsheet()
                spreadsheet.del_worksheet(ws)
                deleted_count += 1
                time.sleep(0.5)
        
        if deleted_count > 0:
            logging.info(f"✅ {deleted_count} feuille(s) '_Technical' supprimée(s)")
        else:
            logging.info("ℹ️  Aucune feuille '_Technical' à supprimer")
    
    except Exception as e:
        logging.error(f"❌ Erreur nettoyage feuilles: {e}")

# ═══════════════════════════════════════════════════════════
# FONCTION PRINCIPALE (OPTIMISÉE)
# ═══════════════════════════════════════════════════════════
def run_data_collection():
    logging.info("="*60)
    logging.info("📊 ÉTAPE 1: COLLECTE OPTIMISÉE (BATCH PROCESSING)")
    logging.info("="*60)
    
    # Connexions
    conn = connect_to_db()
    if not conn:
        return
    
    gc = authenticate_gsheets()
    if not gc:
        logging.warning("⚠️  Google Sheets non disponible, PostgreSQL uniquement")
    
    try:
        spreadsheet = None
        if gc:
            rate_limit_gsheet()
            spreadsheet = gc.open_by_key(SPREADSHEET_ID)
            cleanup_technical_sheets(gc, spreadsheet)
        
        # Récupération des IDs sociétés
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
        
        # ═══════════════════════════════════════════════════════════
        # OPTIMISATION : Charger TOUTES les dates en BATCH
        # ═══════════════════════════════════════════════════════════
        existing_dates = load_existing_dates_batch(gc, spreadsheet, company_ids.keys())
        
        # Récupération de TOUS les BOCs
        boc_links = get_all_boc_links()
        
        if not boc_links:
            logging.error("❌ Aucun BOC trouvé")
            return
        
        total_db_inserts = 0
        total_skipped = 0
        
        # Buffer pour batch updates Google Sheets
        gsheet_updates_buffer = defaultdict(list)  # {symbol: [[date, prix, volume], ...]}
        
        # ═══════════════════════════════════════════════════════════
        # Traiter chaque BOC
        # ═══════════════════════════════════════════════════════════
        for boc_url in boc_links:
            date_str = extract_date_from_url(boc_url)
            
            if not date_str:
                continue
            
            try:
                trade_date = datetime.strptime(date_str, '%Y%m%d').date()
            except ValueError:
                continue
            
            logging.info(f"\n📅 Traitement du BOC du {trade_date.strftime('%d/%m/%Y')}")
            
            date_str_formatted = trade_date.strftime('%d/%m/%Y')
            
            # Vérifier si date existe dans DB
            db_has_date = date_exists_in_db(conn, trade_date)
            
            # ═══════════════════════════════════════════════════════════
            # CAS 1 : Date existe dans DB
            # ═══════════════════════════════════════════════════════════
            if db_has_date:
                logging.info(f"   ✓ Date présente dans DB")
                
                if gc and spreadsheet:
                    # Identifier les feuilles qui manquent la date
                    missing_symbols = [
                        symbol for symbol in company_ids.keys() 
                        if date_str_formatted not in existing_dates[symbol]
                    ]
                    
                    if missing_symbols:
                        logging.info(f"   ⚠️  {len(missing_symbols)} feuille(s) à compléter")
                        
                        # Extraire du PDF pour compléter
                        rows = extract_data_from_pdf(boc_url)
                        
                        if rows:
                            for rec in rows:
                                symbol = rec.get('Symbole', '').strip()
                                
                                if symbol not in missing_symbols:
                                    continue
                                
                                try:
                                    price = clean_and_convert_numeric(rec.get('Cours'))
                                    volume = int(clean_and_convert_numeric(rec.get('Volume')) or 0)
                                    
                                    # Ajouter au buffer (pas d'insertion immédiate)
                                    gsheet_updates_buffer[symbol].append([
                                        symbol, 
                                        date_str_formatted, 
                                        price if price else '', 
                                        volume if volume else ''
                                    ])
                                    
                                    # Mettre à jour le cache
                                    existing_dates[symbol].add(date_str_formatted)
                                
                                except Exception as e:
                                    logging.error(f"   ❌ Erreur {symbol}: {e}")
                    else:
                        total_skipped += 1
                else:
                    total_skipped += 1
                
                continue  # Passer au BOC suivant
            
            # ═══════════════════════════════════════════════════════════
            # CAS 2 : Date n'existe pas dans DB
            # ═══════════════════════════════════════════════════════════
            logging.info(f"   ℹ️  Date absente dans DB, extraction du PDF...")
            rows = extract_data_from_pdf(boc_url)
            
            if not rows:
                logging.warning(f"   ⚠️  Aucune donnée extraite pour {trade_date}")
                continue
            
            db_inserts = 0
            
            for rec in rows:
                symbol = rec.get('Symbole', '').strip()
                
                if symbol not in company_ids:
                    continue
                
                try:
                    price = clean_and_convert_numeric(rec.get('Cours'))
                    volume = int(clean_and_convert_numeric(rec.get('Volume')) or 0)
                    value = clean_and_convert_numeric(rec.get('Valeur'))
                    
                    # Insertion DB
                    if insert_into_db(conn, company_ids, symbol, trade_date, price, volume, value):
                        db_inserts += 1
                    
                    # Ajouter au buffer Google Sheets
                    if gc and spreadsheet:
                        gsheet_updates_buffer[symbol].append([
                            symbol, 
                            date_str_formatted, 
                            price if price else '', 
                            volume if volume else ''
                        ])
                        existing_dates[symbol].add(date_str_formatted)
                
                except Exception as e:
                    logging.error(f"   ❌ Erreur traitement {symbol}: {e}")
                    continue
            
            total_db_inserts += db_inserts
            logging.info(f"   ✅ DB: {db_inserts} inserts")
            time.sleep(0.5)
        
        # ═══════════════════════════════════════════════════════════
        # FLUSH BATCH : Écrire toutes les mises à jour GSheets
        # ═══════════════════════════════════════════════════════════
        total_gsheet_inserts = 0
        if gc and spreadsheet and gsheet_updates_buffer:
            total_gsheet_inserts = batch_update_gsheets(gc, spreadsheet, gsheet_updates_buffer)
        
        # ═══════════════════════════════════════════════════════════
        # Résumé final
        # ═══════════════════════════════════════════════════════════
        logging.info("\n" + "="*60)
        logging.info("✅ COLLECTE TERMINÉE")
        logging.info(f"📊 BOCs traités: {len(boc_links)}")
        logging.info(f"📊 BOCs complets (à jour): {total_skipped}")
        logging.info(f"💾 PostgreSQL: {total_db_inserts} nouveaux enregistrements")
        logging.info(f"📋 Google Sheets: {total_gsheet_inserts} lignes ajoutées")
        logging.info(f"🔧 Requêtes API GSheets: ~{len(request_timestamps)} (limite: 60/min)")
        logging.info("="*60)
    
    except Exception as e:
        logging.error(f"❌ Erreur critique: {e}", exc_info=True)
        if conn:
            conn.rollback()
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_data_collection()
