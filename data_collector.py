# ==============================================================================
# MODULE: DATA COLLECTOR V4.0 - COLLECTE COMPLÈTE SANS DOUBLONS
# ==============================================================================

import re
import time
import logging
import os
import json
from io import BytesIO
from datetime import datetime, timedelta

import pdfplumber
import requests
from bs4 import BeautifulSoup
import psycopg2
import urllib3
import gspread
from google.oauth2 import service_account

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# --- Connexion DB ---
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
    """Extrait la date au format YYYYMMDD depuis l'URL"""
    date_match = re.search(r'boc_(\d{8})', url)
    if date_match:
        return date_match.group(1)
    return None

# --- Récupération TOUS les BOCs disponibles ---
def get_all_boc_links():
    """Récupère tous les BOCs disponibles sur le site"""
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
        
        # Trier par date (du plus ancien au plus récent)
        sorted_links = sorted(list(links), key=lambda x: extract_date_from_url(x) or '19000101')
        
        logging.info(f"✅ {len(sorted_links)} BOC(s) trouvé(s)")
        for link in sorted_links:
            date_str = extract_date_from_url(link)
            logging.info(f"   • BOC du {date_str}")
        
        return sorted_links
    
    except Exception as e:
        logging.error(f"❌ Erreur récupération BOCs: {e}")
        return []

# --- Vérification existence date dans DB ---
def date_exists_in_db(conn, trade_date):
    """Vérifie si des données existent déjà pour cette date"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
            return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"❌ Erreur vérification date DB: {e}")
        return False

# --- Vérification existence date dans Google Sheets ---
def date_exists_in_gsheet(worksheet, trade_date):
    """Vérifie si des données existent déjà pour cette date dans la feuille"""
    try:
        date_str = trade_date.strftime('%d/%m/%Y')
        all_values = worksheet.col_values(2)  # Colonne B = Date
        return date_str in all_values
    except Exception as e:
        logging.error(f"❌ Erreur vérification date GSheet: {e}")
        return False

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
    """Extrait les données du BOC PDF"""
    logging.info(f"📄 Analyse du PDF: {os.path.basename(pdf_url)}")
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
    """Insert les données dans PostgreSQL"""
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

# --- Insertion dans Google Sheets ---
def insert_into_gsheet(gc, spreadsheet, symbol, trade_date, price, volume, value):
    """Insert les données dans Google Sheets à la bonne position chronologique"""
    try:
        worksheet = spreadsheet.worksheet(symbol)
        
        # Préparer les données
        date_str = trade_date.strftime('%d/%m/%Y')
        new_row = [symbol, date_str, price if price else '', volume if volume else '', value if value else '']
        
        # Récupérer toutes les dates existantes
        all_values = worksheet.get_all_values()
        
        if len(all_values) <= 1:  # Seulement l'en-tête ou vide
            worksheet.append_row(new_row, value_input_option='USER_ENTERED')
            return True
        
        # Trouver la position d'insertion (ordre chronologique croissant)
        insert_position = None
        
        for idx, row in enumerate(all_values[1:], start=2):  # Commencer à la ligne 2
            if len(row) < 2:
                continue
            
            try:
                existing_date = datetime.strptime(row[1], '%d/%m/%Y').date()
                
                # Si la date existe déjà, ne rien faire
                if existing_date == trade_date:
                    return False
                
                # Si on trouve une date postérieure, insérer avant
                if existing_date > trade_date:
                    insert_position = idx
                    break
            
            except:
                continue
        
        # Insérer à la position trouvée ou à la fin
        if insert_position:
            worksheet.insert_row(new_row, insert_position, value_input_option='USER_ENTERED')
        else:
            worksheet.append_row(new_row, value_input_option='USER_ENTERED')
        
        return True
    
    except gspread.exceptions.WorksheetNotFound:
        logging.warning(f"⚠️  Feuille '{symbol}' non trouvée")
        return False
    
    except Exception as e:
        logging.error(f"❌ Erreur insertion GSheet pour {symbol}: {e}")
        return False

# --- Nettoyage des feuilles "_Technical" ---
def cleanup_technical_sheets(gc, spreadsheet):
    """Supprime toutes les feuilles se terminant par '_Technical'"""
    try:
        worksheets = spreadsheet.worksheets()
        deleted_count = 0
        
        for ws in worksheets:
            if ws.title.endswith('_Technical'):
                logging.info(f"🗑️  Suppression de la feuille: {ws.title}")
                spreadsheet.del_worksheet(ws)
                deleted_count += 1
                time.sleep(0.5)  # Pause pour respecter les limites API
        
        if deleted_count > 0:
            logging.info(f"✅ {deleted_count} feuille(s) '_Technical' supprimée(s)")
        else:
            logging.info("ℹ️  Aucune feuille '_Technical' à supprimer")
    
    except Exception as e:
        logging.error(f"❌ Erreur nettoyage feuilles: {e}")

# --- Fonction principale ---
def run_data_collection():
    logging.info("="*60)
    logging.info("📊 ÉTAPE 1: COLLECTE COMPLÈTE DES DONNÉES (V4.0)")
    logging.info("="*60)
    
    # Connexions
    conn = connect_to_db()
    if not conn:
        return
    
    gc = authenticate_gsheets()
    if not gc:
        logging.error("❌ Google Sheets requis, arrêt du processus")
        conn.close()
        return
    
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        # Nettoyage des feuilles "_Technical"
        cleanup_technical_sheets(gc, spreadsheet)
        
        # Récupération des IDs sociétés
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
        
        # Récupération de TOUS les BOCs
        boc_links = get_all_boc_links()
        
        if not boc_links:
            logging.error("❌ Aucun BOC trouvé")
            return
        
        total_db_inserts = 0
        total_gsheet_inserts = 0
        total_skipped = 0
        
        # Traiter chaque BOC (du plus ancien au plus récent)
        for boc_url in boc_links:
            date_str = extract_date_from_url(boc_url)
            
            if not date_str:
                continue
            
            try:
                trade_date = datetime.strptime(date_str, '%Y%m%d').date()
            except ValueError:
                continue
            
            logging.info(f"\n📅 Traitement du BOC du {trade_date.strftime('%d/%m/%Y')}")
            
            # Vérifier si cette date existe déjà dans DB
            if date_exists_in_db(conn, trade_date):
                logging.info(f"   ⏭️  Date déjà présente dans DB, passage au suivant")
                total_skipped += 1
                continue
            
            # Extraire les données du PDF
            rows = extract_data_from_pdf(boc_url)
            
            if not rows:
                logging.warning(f"   ⚠️  Aucune donnée extraite pour {trade_date}")
                continue
            
            db_inserts = 0
            gsheet_inserts = 0
            
            # Traiter chaque ligne
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
                    
                    # Insertion Google Sheets
                    if insert_into_gsheet(gc, spreadsheet, symbol, trade_date, price, volume, value):
                        gsheet_inserts += 1
                    
                    time.sleep(0.1)  # Petite pause entre insertions
                
                except Exception as e:
                    logging.error(f"   ❌ Erreur traitement {symbol}: {e}")
                    continue
            
            total_db_inserts += db_inserts
            total_gsheet_inserts += gsheet_inserts
            
            logging.info(f"   ✅ DB: {db_inserts} | GSheet: {gsheet_inserts}")
            time.sleep(1)  # Pause entre BOCs
        
        # Résumé final
        logging.info("\n" + "="*60)
        logging.info("✅ COLLECTE TERMINÉE")
        logging.info(f"📊 BOCs traités: {len(boc_links)}")
        logging.info(f"📊 BOCs ignorés (doublons): {total_skipped}")
        logging.info(f"💾 PostgreSQL: {total_db_inserts} nouveaux enregistrements")
        logging.info(f"📋 Google Sheets: {total_gsheet_inserts} nouveaux enregistrements")
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
