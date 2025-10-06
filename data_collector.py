# ==============================================================================
# MODULE: DATA COLLECTOR (V2.3 - DÉTECTION AMÉLIORÉE)
# ==============================================================================

import re
import time
import unicodedata
import logging
import os
from io import BytesIO
from datetime import datetime

import pdfplumber
import requests
from bs4 import BeautifulSoup
import psycopg2
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

def connect_to_db():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        logging.info("✅ Connexion à la base de données PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à la DB: {e}")
        return None

def get_company_ids(cur):
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur lors de la récupération des IDs des sociétés : {e}")
        return {}

def get_boc_links():
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"Recherche de nouveaux bulletins sur : {url}")
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
    return sorted(list(links), reverse=True)[:15] # Analyser les 15 plus récents pour être sûr

def clean_and_convert_numeric(value):
    if value is None or value == '': return None
    cleaned_value = re.sub(r'\s+', '', str(value)).replace(',', '.')
    try: return float(cleaned_value)
    except (ValueError, TypeError): return None

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
                        if len(row) < 8: continue
                        vol, val = row[-8], row[-7]
                        cours = row[-6] if len(row) >= 6 else ""
                        symbole = row[1] if len(row) > 1 and row[1] and len(row[1]) <= 5 else row[0]
                        if re.search(r'\d', str(vol)) or re.search(r'\d', str(val)):
                            data.append({"Symbole": symbole, "Cours": cours, "Volume": vol, "Valeur": val})
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données du PDF {pdf_url}: {e}")
    return data

def run_data_collection():
    logging.info("="*60)
    logging.info("ÉTAPE 1 : DÉMARRAGE DE LA COLLECTE DE DONNÉES (V2.3)")
    logging.info("="*60)
    
    conn = connect_to_db()
    if not conn: return
        
    try:
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
        
        boc_links = get_boc_links()
        logging.info(f"{len(boc_links)} BOCs récents trouvés sur le site.")
        
        new_records_count = 0
        
        for boc in boc_links:
            date_match = re.search(r'(\d{8})', boc)
            if not date_match: continue
            date_yyyymmdd = date_match.group(1)

            try:
                trade_date = datetime.strptime(date_yyyymmdd, '%Y%m%d').date()
            except ValueError:
                continue

            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
                if cur.fetchone():
                    logging.info(f"Les données pour la date {trade_date} existent déjà. Ignoré.")
                    continue
            
            logging.info(f"Traitement des données pour la date {trade_date}...")
            rows = extract_data_from_pdf(boc)
            if not rows: continue

            with conn.cursor() as cur:
                for rec in rows:
                    symbol = rec.get('Symbole', '').strip()
                    if symbol in company_ids:
                        company_id = company_ids[symbol]
                        try:
                            price = clean_and_convert_numeric(rec.get('Cours'))
                            volume = int(clean_and_convert_numeric(rec.get('Volume')) or 0)
                            value = clean_and_convert_numeric(rec.get('Valeur'))
                            
                            cur.execute("""
                                INSERT INTO historical_data (company_id, trade_date, price, volume, value)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (company_id, trade_date) DO NOTHING;
                            """, (company_id, trade_date, price, volume, value))
                            
                            if cur.rowcount > 0:
                                new_records_count += 1
                                
                        except (ValueError, TypeError) as e:
                            logging.warning(f"Donnée invalide pour {symbol} le {trade_date}, ignorée. Erreur: {e}")
            conn.commit()

        logging.info(f"✅ {new_records_count} nouveaux enregistrements de cours ont été ajoutés à la base de données.")

    except Exception as e:
        logging.error(f"❌ Erreur critique dans la collecte de données : {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
    
    logging.info("Processus de collecte de données terminé.")

if __name__ == "__main__":
    run_data_collection()
