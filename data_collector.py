# ==============================================================================
# MODULE: DATA COLLECTOR (V2.6 - VERSION CORRIGÉE)
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
    """Établit une connexion à la base de données PostgreSQL."""
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

def get_company_ids(cur):
    """Récupère le mapping symbole -> ID depuis la table companies."""
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur lors de la récupération des IDs des sociétés : {e}")
        return {}

def extract_date_from_filename_for_sorting(url):
    """Extrait une date numérique YYYYMMDD pour le tri, avec validation."""
    date_match = re.search(r'(\d{8})', url)
    if date_match:
        date_str = date_match.group(1)
        # Validation de la date
        try:
            datetime.strptime(date_str, '%Y%m%d')
            return date_str
        except ValueError:
            logging.warning(f"Date invalide extraite: {date_str} de {url}")
    
    # Si pas de date valide, utiliser une valeur de tri très ancienne
    logging.debug(f"Impossible d'extraire une date valide de: {url}")
    return '19000101'

def get_boc_links():
    """Récupère les liens vers les bulletins officiels de la cote (BOC)."""
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"🔍 Recherche de bulletins sur : {url}")
    
    try:
        r = requests.get(url, verify=False, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        links = set()
        
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            # CORRECTION : Accepter tous les PDF qui contiennent 'boc_' (avec ou sans suffixe _2)
            if 'boc_' in href.lower() and href.endswith('.pdf'):
                full_url = href if href.startswith('http') else "https://www.brvm.org" + href
                links.add(full_url)
        
        if not links:
            logging.warning("⚠️ Aucun lien de bulletin (BOC) trouvé sur la page principale.")
            return []
        
        # Tri par date décroissante (plus récent en premier) pour l'affichage
        # MAIS on va inverser après pour traiter du plus ancien au plus récent
        sorted_links = sorted(list(links), key=extract_date_from_filename_for_sorting, reverse=True)
        
        logging.info(f"✅ {len(sorted_links)} bulletins identifiés sur le site (incluant les versions _2)")
        
        # Retourner TOUS les bulletins (pas de limite)
        return sorted_links
    
    except Exception as e:
        logging.error(f"❌ Erreur lors de la récupération des liens BOC : {e}")
        return []

def clean_and_convert_numeric(value):
    """Nettoie et convertit une valeur en nombre flottant."""
    if value is None or value == '':
        return None
    
    cleaned_value = re.sub(r'\s+', '', str(value)).replace(',', '.')
    
    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return None

def extract_data_from_pdf(pdf_url):
    """Extrait les données de marché d'un PDF BOC."""
    logging.info(f"📄 Analyse du PDF : {os.path.basename(pdf_url)}")
    data = []
    
    try:
        r = requests.get(pdf_url, verify=False, timeout=30)
        r.raise_for_status()
        pdf_file = BytesIO(r.content)
        
        with pdfplumber.open(pdf_file) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                tables = page.extract_tables() or []
                
                for table in tables:
                    for row in table:
                        # Nettoyer les cellules
                        row = [(cell.strip() if cell else "") for cell in row]
                        
                        # Ignorer les lignes trop courtes
                        if len(row) < 8:
                            continue
                        
                        # Les colonnes Volume et Valeur sont typiquement à -8 et -7
                        vol = row[-8]
                        val = row[-7]
                        cours = row[-6] if len(row) >= 6 else ""
                        
                        # Le symbole est généralement dans les premières colonnes
                        symbole = row[1] if len(row) > 1 and row[1] and len(row[1]) <= 5 else row[0]
                        
                        # Garder seulement les lignes avec des données numériques
                        if re.search(r'\d', str(vol)) or re.search(r'\d', str(val)):
                            data.append({
                                "Symbole": symbole,
                                "Cours": cours,
                                "Volume": vol,
                                "Valeur": val
                            })
        
        logging.info(f"   ✓ {len(data)} lignes de données extraites")
        
    except Exception as e:
        logging.error(f"❌ Erreur lors de l'extraction des données du PDF : {e}")
    
    return data

def run_data_collection():
    """Fonction principale pour la collecte de données."""
    logging.info("=" * 60)
    logging.info("ÉTAPE 1 : DÉMARRAGE DE LA COLLECTE DE DONNÉES (V2.6)")
    logging.info("=" * 60)
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # Récupérer le mapping des IDs de sociétés
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
        
        if not company_ids:
            logging.error("❌ Aucune société trouvée dans la base de données.")
            return
        
        logging.info(f"📊 {len(company_ids)} sociétés identifiées dans la base")
        
        # Récupérer les liens vers les BOCs
        boc_links = get_boc_links()
        
        if not boc_links:
            logging.warning("⚠️ Aucun bulletin trouvé. Fin de la collecte.")
            return
        
        logging.info(f"✅ {len(boc_links)} BOCs identifiés et triés (du plus ancien au plus récent)")
        
        total_new_records = 0
        dates_processed = []
        
        # Traiter chaque bulletin
        for idx, boc in enumerate(boc_links, 1):
            # Extraire la date du nom du fichier
            date_match = re.search(r'(\d{8})', boc)
            if not date_match:
                logging.warning(f"   ⚠️ Impossible d'extraire la date de {boc}. Ignoré.")
                continue
            
            date_yyyymmdd = date_match.group(1)
            
            try:
                trade_date = datetime.strptime(date_yyyymmdd, '%Y%m%d').date()
            except ValueError:
                logging.warning(f"   ⚠️ Format de date invalide : {date_yyyymmdd}. Ignoré.")
                continue
            
            # Vérifier si on a déjà des données pour cette date
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM historical_data WHERE trade_date = %s;", (trade_date,))
                count = cur.fetchone()[0]
                
                if count > 0:
                    logging.info(f"   ⏭️  [{idx}/{len(boc_links)}] {trade_date} : Données déjà présentes ({count} enregistrements). Ignoré.")
                    continue
            
            logging.info(f"   🔄 [{idx}/{len(boc_links)}] Traitement des données pour le {trade_date}...")
            
            # Extraire les données du PDF
            rows = extract_data_from_pdf(boc)
            
            if not rows:
                logging.warning(f"      ⚠️ Aucune donnée extraite du bulletin.")
                continue
            
            # Insérer les données dans la base
            new_records_for_this_date = 0
            
            with conn.cursor() as cur:
                for rec in rows:
                    symbol = rec.get('Symbole', '').strip().upper()
                    
                    if symbol not in company_ids:
                        continue  # Ignorer les symboles non reconnus
                    
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
                            new_records_for_this_date += 1
                    
                    except (ValueError, TypeError) as e:
                        # Ignorer silencieusement les erreurs de conversion
                        logging.debug(f"      Erreur de conversion pour {symbol}: {e}")
                        continue
            
            # Commit après chaque date
            conn.commit()
            
            if new_records_for_this_date > 0:
                logging.info(f"      ✅ {new_records_for_this_date} nouveaux enregistrements ajoutés")
                total_new_records += new_records_for_this_date
                dates_processed.append(str(trade_date))
            else:
                logging.info(f"      ℹ️  Aucune nouvelle donnée (doublons ignorés)")
            
            # Petit délai entre les téléchargements
            time.sleep(1)
        
        # Résumé final
        logging.info("\n" + "=" * 60)
        logging.info("📊 RÉSUMÉ DE LA COLLECTE")
        logging.info("=" * 60)
        logging.info(f"   • Bulletins traités : {len(boc_links)}")
        logging.info(f"   • Dates ajoutées : {len(dates_processed)}")
        logging.info(f"   • Total nouveaux enregistrements : {total_new_records}")
        
        if dates_processed:
            logging.info(f"   • Dates : {', '.join(dates_processed[-5:])}")  # Afficher les 5 dernières
        
        logging.info("=" * 60)
        logging.info("✅ Processus de collecte de données terminé avec succès")
    
    except Exception as e:
        logging.error(f"❌ Erreur critique dans la collecte de données : {e}", exc_info=True)
        if conn:
            conn.rollback()
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_data_collection()
