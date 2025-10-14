# ==============================================================================
# MODULE: DATA COLLECTOR V6.0 - SUPABASE UNIQUEMENT
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

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

# --- Fonction principale ---
def run_data_collection():
    logging.info("="*60)
    logging.info("📊 ÉTAPE 1: COLLECTE DES DONNÉES (SUPABASE UNIQUEMENT)")
    logging.info("="*60)
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # Récupération des IDs sociétés
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
        
        # Récupération de TOUS les BOCs
        boc_links = get_all_boc_links()
        
        if not boc_links:
            logging.error("❌ Aucun BOC trouvé")
            return
        
        total_db_inserts = 0
        total_skipped = 0
        
        # Traiter chaque BOC
        for boc_url in boc_links:
            date_str = extract_date_from_url(boc_url)
            
            if not date_str:
                continue
            
            try:
                trade_date = datetime.strptime(date_str, '%Y%m%d').date()
            except ValueError:
                continue
            
            logging.info(f"\n📅 Traitement du BOC du {trade_date.strftime('%d/%m/%Y')}")
            
            # Vérifier si date existe dans DB
            db_has_date = date_exists_in_db(conn, trade_date)
            
            if db_has_date:
                logging.info(f"   ✓ Date déjà présente dans DB")
                total_skipped += 1
                continue
            
            # Date n'existe pas dans DB, extraction du PDF
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
                
                except Exception as e:
                    logging.error(f"   ❌ Erreur traitement {symbol}: {e}")
                    continue
            
            total_db_inserts += db_inserts
            logging.info(f"   ✅ DB: {db_inserts} inserts")
            time.sleep(0.5)
        
        # Résumé final
        logging.info("\n" + "="*60)
        logging.info("✅ COLLECTE TERMINÉE")
        logging.info(f"📊 BOCs traités: {len(boc_links)}")
        logging.info(f"📊 BOCs déjà en base: {total_skipped}")
        logging.info(f"💾 PostgreSQL: {total_db_inserts} nouveaux enregistrements")
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
