# ==============================================================================
# MODULE: DATA COLLECTOR V30.0 - SIMPLIFIÉ (6 variables uniquement) - VERSION CORRIGÉE
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")

# Configuration
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")


def connect_to_db():
    """Connexion PostgreSQL"""
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


def get_company_ids(cur):
    """Récupération des IDs sociétés"""
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur récupération IDs: {e}")
        return {}


def extract_date_from_url(url):
    """Extraction date depuis URL"""
    date_match = re.search(r"boc_(\d{8})", url)
    return date_match.group(1) if date_match else None


def get_all_boc_links():
    """Récupération des liens BOC"""
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"🔍 Recherche BOCs sur : {url}")
    
    try:
        r = requests.get(url, verify=False, timeout=30)
        soup = BeautifulSoup(r.content, "html.parser")
        links = set()
        
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "boc_" in href.lower() and href.endswith(".pdf"):
                full_url = href if href.startswith("http") else "https://www.brvm.org" + href
                links.add(full_url)
        
        if not links:
            logging.warning("⚠️ Aucun BOC trouvé")
            return []
        
        sorted_links = sorted(list(links), key=lambda x: extract_date_from_url(x) or "19000101")
        logging.info(f"✅ {len(sorted_links)} BOC(s) trouvé(s)")
        return sorted_links
    except Exception as e:
        logging.error(f"❌ Erreur récupération BOCs: {e}")
        return []


def date_exists_in_db(conn, trade_date):
    """Vérification date dans DB"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
            return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"❌ Erreur vérification date: {e}")
        return False


def clean_and_convert_numeric(value):
    """Nettoyage et conversion valeurs numériques (virgule → point)"""
    if value is None or value == "":
        return None
    
    cleaned_value = str(value).strip()
    cleaned_value = re.sub(r'\s+', '', cleaned_value)
    cleaned_value = cleaned_value.replace(',', '.')
    
    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return None


def extract_data_from_pdf(pdf_url):
    """Extraction données depuis PDF"""
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
                        
                        if re.search(r"\d", str(vol)) or re.search(r"\d", str(val)):
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


def extract_market_indicators(pdf_text: str) -> dict:
    """
    ✅ VERSION CORRIGÉE - Extraction des 6 indicateurs avec regex robustes
    """
    indicators = {}
    
    # 🔧 FIX 1: BRVM COMPOSITE (fonctionne déjà bien)
    match = re.search(r"BRVM\s+COMPOSITE\s+([\d\s,\.]+)", pdf_text, re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        indicators["brvm_composite"] = re.sub(r'\s+', '', raw).replace(',', '.')
        logging.info(f"   ✓ BRVM Composite trouvé: {indicators['brvm_composite']}")
    else:
        indicators["brvm_composite"] = None
        logging.warning("   ⚠️ BRVM Composite NON trouvé")
    
    # 🔧 FIX 2: BRVM 30 (fonctionne déjà bien)
    match = re.search(r"BRVM\s+30\s+([\d\s,\.]+)", pdf_text, re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        indicators["brvm_30"] = re.sub(r'\s+', '', raw).replace(',', '.')
        logging.info(f"   ✓ BRVM 30 trouvé: {indicators['brvm_30']}")
    else:
        indicators["brvm_30"] = None
        logging.warning("   ⚠️ BRVM 30 NON trouvé")
    
    # 🔧 FIX 3: BRVM PRESTIGE (CORRECTION MAJEURE - avec espace OU tiret)
    match = re.search(r"BRVM[\s\-]+PRESTIGE\s+([\d\s,\.]+)", pdf_text, re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        indicators["brvm_prestige"] = re.sub(r'\s+', '', raw).replace(',', '.')
        logging.info(f"   ✓ BRVM Prestige trouvé: {indicators['brvm_prestige']}")
    else:
        indicators["brvm_prestige"] = None
        logging.warning("   ⚠️ BRVM Prestige NON trouvé")
    
    # 🔧 FIX 4: CAPITALISATION GLOBALE (CORRECTION MAJEURE)
    # Recherche dans tableau "Actions" -> ligne "Capitalisation boursière"
    match = re.search(
        r"Capitalisation\s+boursière\s+\(FCFA\)\s*\(Actions\s*[&\+]?\s*Droits\)\s+([\d\s]+)",
        pdf_text,
        re.IGNORECASE | re.DOTALL
    )
    if match:
        raw = match.group(1).strip()
        # Nettoyer TOUS les espaces
        cleaned = re.sub(r'\s+', '', raw)
        indicators["capitalisation_globale"] = cleaned
        logging.info(f"   ✓ Capitalisation Globale trouvée: {indicators['capitalisation_globale']}")
    else:
        # Alternative: chercher juste après "Actions"
        match_alt = re.search(
            r"Actions\s+Niveau\s+Evol\.\s+Jour\s+Capitalisation\s+boursière[^\d]+([\d\s]+)",
            pdf_text,
            re.IGNORECASE | re.DOTALL
        )
        if match_alt:
            raw = match_alt.group(1).strip()
            cleaned = re.sub(r'\s+', '', raw)
            indicators["capitalisation_globale"] = cleaned
            logging.info(f"   ✓ Capitalisation Globale trouvée (alt): {indicators['capitalisation_globale']}")
        else:
            indicators["capitalisation_globale"] = None
            logging.warning("   ⚠️ Capitalisation Globale NON trouvée")
    
    # 🔧 FIX 5: VOLUME MOYEN ANNUEL (fonctionne déjà bien)
    match = re.search(r"Volume\s+moyen\s+annuel\s+par\s+séance\s+([\d\s,\.]+)", pdf_text, re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        indicators["volume_moyen_annuel"] = re.sub(r'\s+', '', raw).replace(',', '.')
        logging.info(f"   ✓ Volume Moyen Annuel trouvé: {indicators['volume_moyen_annuel']}")
    else:
        indicators["volume_moyen_annuel"] = None
        logging.warning("   ⚠️ Volume Moyen Annuel NON trouvé")
    
    # 🔧 FIX 6: VALEUR MOYENNE ANNUELLE (fonctionne déjà bien)
    match = re.search(r"Valeur\s+moyenne\s+annuelle\s+par\s+séance\s+([\d\s,\.]+)", pdf_text, re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        indicators["valeur_moyenne_annuelle"] = re.sub(r'\s+', '', raw).replace(',', '.')
        logging.info(f"   ✓ Valeur Moyenne Annuelle trouvée: {indicators['valeur_moyenne_annuelle']}")
    else:
        indicators["valeur_moyenne_annuelle"] = None
        logging.warning("   ⚠️ Valeur Moyenne Annuelle NON trouvée")
    
    return indicators


def insert_into_db(conn, company_ids, symbol, trade_date, price, volume, value):
    """Insertion historical_data"""
    if symbol not in company_ids:
        return False
    
    company_id = company_ids[symbol]
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO historical_data (company_id, trade_date, price, volume, value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (company_id, trade_date) DO NOTHING;
                """,
                (company_id, trade_date, price, volume, value)
            )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        logging.error(f"❌ Erreur insertion: {e}")
        conn.rollback()
        return False


def insert_market_indicators_to_db(conn, indicators, trade_date):
    """
    Insertion UNIQUEMENT des 6 variables demandées dans new_market_indicators
    """
    try:
        with conn.cursor() as cursor:
            # Conversion avec gestion virgule
            brvm_composite = clean_and_convert_numeric(indicators.get("brvm_composite"))
            brvm_30 = clean_and_convert_numeric(indicators.get("brvm_30"))
            brvm_prestige = clean_and_convert_numeric(indicators.get("brvm_prestige"))
            capitalisation_globale = clean_and_convert_numeric(indicators.get("capitalisation_globale"))
            volume_moyen_annuel = clean_and_convert_numeric(indicators.get("volume_moyen_annuel"))
            valeur_moyenne_annuelle = clean_and_convert_numeric(indicators.get("valeur_moyenne_annuelle"))
            
            # Log des valeurs avant insertion
            logging.info(f"   📊 Valeurs à insérer:")
            logging.info(f"      • BRVM Composite: {brvm_composite}")
            logging.info(f"      • BRVM 30: {brvm_30}")
            logging.info(f"      • BRVM Prestige: {brvm_prestige}")
            logging.info(f"      • Capitalisation: {capitalisation_globale}")
            logging.info(f"      • Volume Moyen: {volume_moyen_annuel}")
            logging.info(f"      • Valeur Moyenne: {valeur_moyenne_annuelle}")
            
            # ✅ INSERT simple - 6 variables seulement
            insert_query = """
                INSERT INTO new_market_indicators (
                    extraction_date,
                    brvm_composite,
                    brvm_30,
                    brvm_prestige,
                    capitalisation_globale,
                    volume_moyen_annuel,
                    valeur_moyenne_annuelle
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (extraction_date) DO UPDATE SET
                    brvm_composite = EXCLUDED.brvm_composite,
                    brvm_30 = EXCLUDED.brvm_30,
                    brvm_prestige = EXCLUDED.brvm_prestige,
                    capitalisation_globale = EXCLUDED.capitalisation_globale,
                    volume_moyen_annuel = EXCLUDED.volume_moyen_annuel,
                    valeur_moyenne_annuelle = EXCLUDED.valeur_moyenne_annuelle;
            """
            
            values = (
                trade_date,
                brvm_composite,
                brvm_30,
                brvm_prestige,
                capitalisation_globale,
                volume_moyen_annuel,
                valeur_moyenne_annuelle
            )
            
            cursor.execute(insert_query, values)
            conn.commit()
            logging.info("✅ Indicateurs de marché insérés avec succès")
    except Exception as e:
        logging.error(f"❌ Erreur insertion indicateurs: {e}")
        conn.rollback()


def run_data_collection():
    """Fonction principale de collecte"""
    logging.info("=" * 60)
    logging.info("📊 ÉTAPE 1: COLLECTE SIMPLIFIÉE (6 VARIABLES) - VERSION CORRIGÉE")
    logging.info("=" * 60)
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        with conn.cursor() as cur:
            company_ids = get_company_ids(cur)
        
        boc_links = get_all_boc_links()
        if not boc_links:
            logging.error("❌ Aucun BOC trouvé")
            return
        
        total_db_inserts = 0
        total_skipped = 0
        
        for boc_url in boc_links:
            date_str = extract_date_from_url(boc_url)
            if not date_str:
                continue
            
            try:
                trade_date = datetime.strptime(date_str, "%Y%m%d").date()
            except ValueError:
                continue
            
            logging.info(f"\n📅 Traitement du BOC du {trade_date.strftime('%d/%m/%Y')}")
            
            if date_exists_in_db(conn, trade_date):
                logging.info("   ✓ Date déjà présente dans DB")
                total_skipped += 1
                continue
            
            logging.info("   ℹ️ Extraction des données du PDF...")
            rows = extract_data_from_pdf(boc_url)
            
            if not rows:
                logging.warning(f"   ⚠️ Aucune donnée extraite")
                continue
            
            # Extraction texte pour indicateurs
            pdf_bytes = requests.get(boc_url, verify=False, timeout=30).content
            try:
                pdf_text = ""
                with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                    for page in pdf.pages:
                        pdf_text += page.extract_text() or ""
            except Exception as e:
                logging.warning(f"⚠️ Impossible d'extraire le texte : {e}")
                pdf_text = ""
            
            # ✅ Insertion indicateurs (6 variables seulement)
            logging.info("   🔍 Extraction des indicateurs de marché...")
            indicators = extract_market_indicators(pdf_text)
            insert_market_indicators_to_db(conn, indicators, trade_date)
            
            # Insertion historical_data
            db_inserts = 0
            for rec in rows:
                symbol = rec.get("Symbole", "").strip()
                if symbol not in company_ids:
                    continue
                
                try:
                    price = clean_and_convert_numeric(rec.get("Cours"))
                    volume = int(clean_and_convert_numeric(rec.get("Volume")) or 0)
                    value = clean_and_convert_numeric(rec.get("Valeur"))
                    
                    if insert_into_db(conn, company_ids, symbol, trade_date, price, volume, value):
                        db_inserts += 1
                except Exception as e:
                    logging.error(f"   ❌ Erreur traitement {symbol}: {e}")
                    continue
            
            total_db_inserts += db_inserts
            logging.info(f"   ✅ DB: {db_inserts} inserts")
            time.sleep(0.5)
        
        logging.info("\n" + "=" * 60)
        logging.info("✅ COLLECTE TERMINÉE")
        logging.info(f"📊 BOCs traités: {len(boc_links)}")
        logging.info(f"📊 BOCs déjà en base: {total_skipped}")
        logging.info(f"💾 PostgreSQL: {total_db_inserts} nouveaux enregistrements")
        logging.info("=" * 60)
    
    except Exception as e:
        logging.error(f"❌ Erreur critique: {e}", exc_info=True)
        if conn:
            conn.rollback()
    
    finally:
        if conn:
            conn.close()


class BRVMDataCollector:
    """Classe wrapper pour compatibilité"""
    def run(self):
        run_data_collection()


if __name__ == "__main__":
    run_data_collection()
