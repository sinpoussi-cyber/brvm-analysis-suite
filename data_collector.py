# ==============================================================================
# MODULE: DATA COLLECTOR (V2.0 - POSTGRESQL)
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
from psycopg2 import sql
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Récupération des Secrets ---
# (Ces variables seront fournies par le workflow GitHub Actions)
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

def normalize_text(s):
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize('NFKD', s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'[^A-Za-z0-9 ]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s.upper()

def extract_date_from_filename(url):
    m = re.search(r'boc_(\d{8})', url, flags=re.IGNORECASE)
    return m.group(1) if m else None

def connect_to_db():
    """Établit la connexion à la base de données PostgreSQL."""
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
        logging.error(f"❌ Impossible de se connecter à la base de données PostgreSQL : {e}")
        return None

def get_company_ids(cur):
    """Récupère tous les symboles et leurs IDs depuis la base de données."""
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur lors de la récupération des IDs des sociétés : {e}")
        return {}

def get_boc_links():
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    r = requests.get(url, verify=False, timeout=30)
    soup = BeautifulSoup(r.content, 'html.parser')
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if re.search(r'boc_\d{8}_\d+\.pdf$', href, flags=re.IGNORECASE):
            full_url = href if href.startswith('http') else "https://www.brvm.org" + href
            links.add(full_url)
    return sorted(list(links), key=lambda u: extract_date_from_filename(u) or '')

def extract_data_from_pdf(pdf_url):
    logging.info(f"Analyse du PDF : {pdf_url}")
    try:
        r = requests.get(pdf_url, verify=False, timeout=30)
        pdf_file = BytesIO(r.content)
        data = []
        with pdfplumber.open(pdf_file) as pdf:
            pages_to_try = [p for p in [2, 3] if p < len(pdf.pages)]
            for pidx in pages_to_try:
                page = pdf.pages[pidx]
                tables = page.extract_tables() or []
                for table in tables:
                    for row in table:
                        row = [(cell.strip() if cell else "") for cell in row]
                        if len(row) < 8: continue
                        vol, val = row[-8], row[-7]
                        cours = row[-6] if len(row) >= 6 else ""
                        symbole = row[1] if len(row) > 1 and row[1] else row[0]
                        if re.search(r'\d', str(vol)) or re.search(r'\d', str(val)):
                            data.append({"Symbole": symbole, "Cours": cours, "Volume": vol, "Valeur": val})
        return data
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données du PDF {pdf_url}: {e}")
        return []

def run_data_collection():
    logging.info("="*60)
    logging.info("ÉTAPE 1 : DÉMARRAGE DE LA COLLECTE DE DONNÉES (VERSION POSTGRESQL)")
    logging.info("="*60)
    
    conn = connect_to_db()
    if not conn:
        return
        
    cur = conn.cursor()
    company_ids = get_company_ids(cur)
    
    boc_links = get_boc_links()
    logging.info(f"{len(boc_links)} BOCs pertinents trouvés.")
    
    new_records_count = 0
    
    for boc in boc_links:
        date_yyyymmdd = extract_date_from_filename(boc)
        if not date_yyyymmdd: continue
        
        try:
            trade_date = datetime.strptime(date_yyyymmdd, '%Y%m%d').date()
        except ValueError:
            logging.warning(f"Format de date invalide pour {boc}, ignoré.")
            continue

        rows = extract_data_from_pdf(boc)
        
        for rec in rows:
            symbol = rec.get('Symbole', '')
            if symbol in company_ids:
                company_id = company_ids[symbol]
                
                try:
                    price = float(str(rec.get('Cours', '0')).replace(',', '.')) if rec.get('Cours') else None
                    volume = int(str(rec.get('Volume', '0')).replace(',', '.')) if rec.get('Volume') else None
                    value = float(str(rec.get('Valeur', '0')).replace(',', '.')) if rec.get('Valeur') else None
                    
                    cur.execute("""
                        INSERT INTO historical_data (company_id, trade_date, price, volume, value)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (company_id, trade_date) DO NOTHING;
                    """, (company_id, trade_date, price, volume, value))
                    
                    if cur.rowcount > 0:
                        new_records_count += 1
                        
                except (ValueError, TypeError) as e:
                    logging.warning(f"Donnée invalide pour {symbol} le {trade_date}, ignorée. Erreur: {e}")
                except Exception as e:
                    logging.error(f"Erreur DB pour {symbol} le {trade_date}: {e}")
                    conn.rollback()

    conn.commit()
    logging.info(f"✅ {new_records_count} nouveaux enregistrements de cours ont été ajoutés à la base de données.")
    
    cur.close()
    conn.close()
    logging.info("Processus de collecte de données terminé.")

if __name__ == "__main__":
    run_data_collection()
