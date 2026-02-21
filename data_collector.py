# ==============================================================================
# MODULE: DATA COLLECTOR V28.0 - CORRIGÉ (Format numérique + Nouveaux secteurs)
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

# ==============================================================================
# --- Configuration & Secrets ---
# ==============================================================================
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")


# ==============================================================================
# Connexion PostgreSQL
# ==============================================================================
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        logging.info("✅ Connexion PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur connexion DB: {e}")
        return None


# ==============================================================================
# Récupération des IDs sociétés
# ==============================================================================
def get_company_ids(cur):
    try:
        cur.execute("SELECT symbol, id FROM companies;")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ Erreur récupération IDs sociétés: {e}")
        return {}


# ==============================================================================
# Extraction date depuis URL
# ==============================================================================
def extract_date_from_url(url):
    date_match = re.search(r"boc_(\d{8})", url)
    if date_match:
        return date_match.group(1)
    return None


# ==============================================================================
# Récupération des BOCs
# ==============================================================================
def get_all_boc_links():
    url = "https://www.brvm.org/fr/bulletins-officiels-de-la-cote"
    logging.info(f"🔍 Recherche de TOUS les BOCs sur : {url}")

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
            logging.warning("⚠️  Aucun BOC trouvé sur la page")
            return []

        sorted_links = sorted(list(links), key=lambda x: extract_date_from_url(x) or "19000101")
        logging.info(f"✅ {len(sorted_links)} BOC(s) trouvé(s)")
        return sorted_links

    except Exception as e:
        logging.error(f"❌ Erreur récupération BOCs: {e}")
        return []


# ==============================================================================
# Vérification date dans DB
# ==============================================================================
def date_exists_in_db(conn, trade_date):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM historical_data WHERE trade_date = %s LIMIT 1;", (trade_date,))
            return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"❌ Erreur vérification date DB: {e}")
        return False


# ==============================================================================
# Nettoyage valeurs numériques - CORRECTION MAJEURE
# ==============================================================================
def clean_and_convert_numeric(value):
    """
    Nettoie et convertit une valeur numérique.
    Gère les formats français (virgule) et anglais (point)
    """
    if value is None or value == "":
        return None
    
    # Convertir en string
    cleaned_value = str(value).strip()
    
    # Supprimer les espaces
    cleaned_value = re.sub(r'\s+', '', cleaned_value)
    
    # CORRECTION: Remplacer la virgule par un point pour les décimales
    cleaned_value = cleaned_value.replace(',', '.')
    
    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return None


# ==============================================================================
# Extraction données depuis PDF (tableaux BOC)
# ==============================================================================
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

                        if re.search(r"\d", str(vol)) or re.search(r"\d", str(val)):
                            data.append(
                                {"Symbole": symbole, "Cours": cours, "Volume": vol, "Valeur": val}
                            )

        logging.info(f"   ✓ {len(data)} ligne(s) extraite(s)")
        return data

    except Exception as e:
        logging.error(f"❌ Erreur extraction PDF: {e}")
        return []


# ==============================================================================
# Extraction des indicateurs de marché depuis PDF - CORRECTION MAJEURE
# ==============================================================================
def extract_market_indicators(pdf_text: str) -> dict:
    """Extrait les indicateurs de marché avec nettoyage des valeurs"""

    def get_value(label, text):
        """Récupère et nettoie une valeur numérique"""
        match = re.search(rf"{label}\s+([\d\s,\.]+)", text)
        if match:
            raw_value = match.group(1).strip()
            # Nettoyer: supprimer espaces, remplacer virgule par point
            cleaned = re.sub(r'\s+', '', raw_value)
            cleaned = cleaned.replace(',', '.')
            return cleaned
        return None

    indicators = {}
    indicators["brvm_composite"] = {"valeur": get_value("BRVM COMPOSITE", pdf_text)}
    indicators["brvm_30"] = {"valeur": get_value("BRVM 30", pdf_text)}
    indicators["brvm_prestige"] = {"valeur": get_value("BRVM-PRESTIGE", pdf_text)}
    indicators["brvm_croissance"] = {"valeur": get_value("BRVM-PRINCIPAL", pdf_text)}
    indicators["capitalisation_globale"] = {
        "valeur": get_value("Capitalisation boursière.*Actions.*Droits", pdf_text)
    }
    indicators["volume_moyen_annuel"] = get_value("Volume moyen annuel par séance", pdf_text)
    indicators["valeur_moyenne_annuelle"] = get_value("Valeur moyenne annuelle par séance", pdf_text)

    return indicators


# ==============================================================================
# Insertion dans DB (historical_data)
# ==============================================================================
def insert_into_db(conn, company_ids, symbol, trade_date, price, volume, value):
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
                (company_id, trade_date, price, volume, value),
            )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        logging.error(f"❌ Erreur insertion: {e}")
        conn.rollback()
        return False


# ==============================================================================
# Insertion des indicateurs de marché - CORRECTION MAJEURE
# ==============================================================================
def insert_market_indicators_to_db(conn, indicators, trade_date):
    """Insère les indicateurs de marché avec conversion correcte des valeurs"""
    
    try:
        with conn.cursor() as cursor:
            # Extraire et convertir les valeurs
            brvm_composite = clean_and_convert_numeric(indicators.get("brvm_composite", {}).get("valeur"))
            brvm_30 = clean_and_convert_numeric(indicators.get("brvm_30", {}).get("valeur"))
            brvm_prestige = clean_and_convert_numeric(indicators.get("brvm_prestige", {}).get("valeur"))
            brvm_croissance = clean_and_convert_numeric(indicators.get("brvm_croissance", {}).get("valeur"))
            capitalisation_globale = clean_and_convert_numeric(indicators.get("capitalisation_globale", {}).get("valeur"))
            volume_moyen_annuel = clean_and_convert_numeric(indicators.get("volume_moyen_annuel"))
            valeur_moyenne_annuelle = clean_and_convert_numeric(indicators.get("valeur_moyenne_annuelle"))

            insert_query = """
                INSERT INTO new_market_indicators (
                    extraction_date, brvm_composite, brvm_30, brvm_prestige, brvm_croissance,
                    capitalisation_globale, volume_moyen_annuel, valeur_moyenne_annuelle
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (extraction_date) DO UPDATE SET
                    brvm_composite = EXCLUDED.brvm_composite,
                    brvm_30 = EXCLUDED.brvm_30,
                    brvm_prestige = EXCLUDED.brvm_prestige,
                    brvm_croissance = EXCLUDED.brvm_croissance,
                    capitalisation_globale = EXCLUDED.capitalisation_globale,
                    volume_moyen_annuel = EXCLUDED.volume_moyen_annuel,
                    valeur_moyenne_annuelle = EXCLUDED.valeur_moyenne_annuelle;
            """

            values = (
                trade_date,
                brvm_composite,
                brvm_30,
                brvm_prestige,
                brvm_croissance,
                capitalisation_globale,
                volume_moyen_annuel,
                valeur_moyenne_annuelle
            )

            cursor.execute(insert_query, values)
            conn.commit()
            logging.info("✅ Indicateurs de marché insérés avec succès.")
    except Exception as e:
        logging.error(f"❌ Erreur insertion indicateurs marché: {e}")
        conn.rollback()


# ==============================================================================
# Calcul des variations journalières et YTD
# ==============================================================================
def update_market_variations(conn):
    """Calcule les variations journalières et YTD pour les indicateurs de marché."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT extraction_date, brvm_prestige, brvm_croissance, brvm_composite, brvm_30,
                       valeur_moyenne_annuelle, volume_moyen_annuel, capitalisation_globale
                FROM new_market_indicators
                ORDER BY extraction_date DESC
                LIMIT 10;
                """
            )
            rows = cur.fetchall()
            if len(rows) < 2:
                logging.warning("⚠️ Pas assez de données pour calculer les variations journalières.")
                return

            latest, prev = rows[0], rows[1]
            latest_date = latest[0]

            def pct_change(new, old):
                try:
                    # Convertir en float en gérant les virgules
                    new_f = float(str(new).replace(",", ".")) if new else 0
                    old_f = float(str(old).replace(",", ".")) if old else 0
                    
                    if old_f == 0:
                        return None
                    return round(((new_f - old_f) / old_f) * 100, 3)
                except Exception:
                    return None

            variations_jour = {
                "variation_journaliere_brvm_prestige": pct_change(latest[1], prev[1]),
                "variation_journaliere_brvm_croissance": pct_change(latest[2], prev[2]),
                "variation_journaliere_brvm_composite": pct_change(latest[3], prev[3]),
                "variation_journaliere_brvm_30": pct_change(latest[4], prev[4]),
                "variation_journaliere_valeur_moyenne_annuelle": pct_change(latest[5], prev[5]),
                "variation_journaliere_volume_moyen_annuel": pct_change(latest[6], prev[6]),
                "variation_journaliere_capitalisation_globale": pct_change(latest[7], prev[7]),
            }

            # Données de l'année précédente pour le calcul YTD
            cur.execute(
                """
                SELECT extraction_date, brvm_prestige, brvm_croissance, brvm_composite, brvm_30,
                       valeur_moyenne_annuelle, volume_moyen_annuel, capitalisation_globale
                FROM new_market_indicators
                WHERE EXTRACT(YEAR FROM extraction_date) = EXTRACT(YEAR FROM CURRENT_DATE) - 1
                ORDER BY extraction_date DESC
                LIMIT 1;
                """
            )
            prev_year = cur.fetchone()

            variations_ytd = {}
            if prev_year:
                for i, col in enumerate(
                    [
                        "brvm_prestige",
                        "brvm_croissance",
                        "brvm_composite",
                        "brvm_30",
                        "valeur_moyenne_annuelle",
                        "volume_moyen_annuel",
                        "capitalisation_globale",
                    ],
                    start=1,
                ):
                    variations_ytd[f"variation_ytd_{col}"] = pct_change(latest[i], prev_year[i])
            else:
                logging.warning("⚠️ Aucune donnée de l'année précédente pour le calcul YTD.")

            updates = {**variations_jour, **variations_ytd}
            set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
            values = list(updates.values()) + [latest_date]

            cur.execute(
                f"""
                UPDATE new_market_indicators
                SET {set_clause}
                WHERE extraction_date = %s;
                """,
                values,
            )
            conn.commit()
            logging.info(f"✅ Variations journalières et YTD mises à jour pour {latest_date}.")
    except Exception as e:
        logging.error(f"❌ Erreur calcul variations: {e}")
        conn.rollback()


# ==============================================================================
# Fonction principale
# ==============================================================================
def run_data_collection():
    logging.info("=" * 60)
    logging.info("📊 ÉTAPE 1: COLLECTE DES DONNÉES (V28.0 CORRIGÉ)")
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

            logging.info("   ℹ️  Extraction des données du PDF...")
            rows = extract_data_from_pdf(boc_url)

            if not rows:
                logging.warning(f"   ⚠️  Aucune donnée extraite pour {trade_date}")
                continue

            pdf_bytes = requests.get(boc_url, verify=False, timeout=30).content
            try:
                pdf_text = ""
                with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                    for page in pdf.pages:
                        pdf_text += page.extract_text() or ""
            except Exception as e:
                logging.warning(f"⚠️ Impossible d'extraire le texte brut du PDF : {e}")
                pdf_text = ""

            indicators = extract_market_indicators(pdf_text)
            insert_market_indicators_to_db(conn, indicators, trade_date)

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

        # Calcul des variations après insertion
        update_market_variations(conn)

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


# ==============================================================================
# Point d'entrée avec classe wrapper pour compatibilité
# ==============================================================================
class BRVMDataCollector:
    """Classe wrapper pour compatibilité avec main.py"""
    def run(self):
        run_data_collection()


if __name__ == "__main__":
    run_data_collection()
