# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER (V3.2 - CORRECTION NOM MODÈLE)
# ==============================================================================

import requests
from bs4 import BeautifulSoup
import time
import re
import os
from datetime import datetime
import logging
import unicodedata
import urllib3
import json
from collections import defaultdict
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2
from psycopg2 import sql
import google.generativeai as genai
from google.api_core import exceptions as api_exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

class BRVMAnalyzer:
    def __init__(self):
        self.societes_mapping = {
            'NTLC': {'nom_rapport': 'NESTLE CI', 'alternatives': ['nestle ci', 'nestle']},
            'PALC': {'nom_rapport': 'PALM CI', 'alternatives': ['palm ci']},
            'TTLC': {'nom_rapport': 'TOTALENERGIES MARKETING CI', 'alternatives': ['totalenergies marketing ci', 'total ci']},
            'TTLS': {'nom_rapport': 'TOTALENERGIES MARKETING SN', 'alternatives': ['totalenergies marketing senegal', 'total senegal']},
            'ECOC': {'nom_rapport': 'ECOBANK COTE D\'IVOIRE', 'alternatives': ['ecobank cote d ivoire', 'ecobank ci']},
            'NSBC': {'nom_rapport': 'NSIA BANQUE CI', 'alternatives': ['nsia banque ci', 'nsbc']},
            'SGBC': {'nom_rapport': 'SOCIETE GENERALE CI', 'alternatives': ['societe generale ci', 'sgb ci']},
            'ONTBF': {'nom_rapport': 'ONATEL BF', 'alternatives': ['onatel bf', 'moov africa']},
            'ORAC': {'nom_rapport': 'ORANGE COTE D\'IVOIRE', 'alternatives': ['orange ci', "orange cote d ivoire"]},
            'SNTS': {'nom_rapport': 'SONATEL SN', 'alternatives': ['sonatel sn', 'fctc sonatel', 'sonatel']},
            'SCRC': {'nom_rapport': 'SUCRIVOIRE', 'alternatives': ['sucrivoire']},
            'SICC': {'nom_rapport': 'SICOR CI', 'alternatives': ['sicor ci', 'sicor']},
            'SLBC': {'nom_rapport': 'SOLIBRA CI', 'alternatives': ['solibra ci', 'solibra']},
            'SOGC': {'nom_rapport': 'SOGB CI', 'alternatives': ['sogb ci', 'sogb']},
            'SPHC': {'nom_rapport': 'SAPH CI', 'alternatives': ['saph ci', 'saph']},
            'STBC': {'nom_rapport': 'SITAB CI', 'alternatives': ['sitab ci', 'sitab']},
            'UNLC': {'nom_rapport': 'UNILEVER CI', 'alternatives': ['unilever ci']},
            'ABJC': {'nom_rapport': 'SERVAIR ABIDJAN CI', 'alternatives': ['servair abidjan ci', 'servair']},
            'BNBC': {'nom_rapport': 'BERNABE CI', 'alternatives': ['bernabe ci']},
            'CFAC': {'nom_rapport': 'CFAO MOTORS CI', 'alternatives': ['cfao motors ci']},
            'LNBB': {'nom_rapport': 'LOTERIE NATIONALE BN', 'alternatives': ['loterie nationale bn', 'lonab']},
            'NEIC': {'nom_rapport': 'NEI-CEDA CI', 'alternatives': ['nei-ceda ci']},
            'PRSC': {'nom_rapport': 'TRACTAFRIC MOTORS CI', 'alternatives': ['tractafric motors ci', 'tractafric']},
            'UNXC': {'nom_rapport': 'UNIWAX CI', 'alternatives': ['uniwax ci']},
            'SHEC': {'nom_rapport': 'VIVO ENERGY CI', 'alternatives': ['vivo energy ci']},
            'SMBC': {'nom_rapport': 'SMB CI', 'alternatives': ['smb ci']},
            'BICB': {'nom_rapport': 'BICI BN', 'alternatives': ['bici bn', 'bicib']},
            'BICC': {'nom_rapport': 'BICI CI', 'alternatives': ['bici ci']},
            'BOAB': {'nom_rapport': 'BANK OF AFRICA BN', 'alternatives': ['bank of africa bn']},
            'BOABF': {'nom_rapport': 'BANK OF AFRICA BF', 'alternatives': ['bank of africa bf']},
            'BOAC': {'nom_rapport': 'BANK OF AFRICA CI', 'alternatives': ['bank of africa ci']},
            'BOAM': {'nom_rapport': 'BANK OF AFRICA ML', 'alternatives': ['bank of africa ml']},
            'BOAN': {'nom_rapport': 'BANK OF AFRICA NG', 'alternatives': ['bank of africa ng']},
            'BOAS': {'nom_rapport': 'BANK OF AFRICA SN', 'alternatives': ['bank of africa sn']},
            'CBIBF': {'nom_rapport': 'CORIS BANKING INTERNATIONAL', 'alternatives': ['coris bank international', 'coris bank']},
            'ETIT': {'nom_rapport': 'ECOBANK TRANSNATIONAL INCORPORATED', 'alternatives': ['ecobank trans', 'ecobank tg']},
            'ORGT': {'nom_rapport': 'ORAGROUP TOGO', 'alternatives': ['oragroup tg', 'oragroup']},
            'SAFC': {'nom_rapport': 'SAFCA CI', 'alternatives': ['safca ci']},
            'SIBC': {'nom_rapport': 'SOCIETE IVOIRIENNE DE BANQUE', 'alternatives': ['societe ivoirienne de banque', 'sib']},
            'CABC': {'nom_rapport': 'SICABLE CI', 'alternatives': ['sicable ci', 'sicable']},
            'FTSC': {'nom_rapport': 'FILTISAC CI', 'alternatives': ['filtisac ci']},
            'SDSC': {'nom_rapport': 'AFRICA GLOBAL LOGISTICS', 'alternatives': ['africa global logistics', 'agl']},
            'SEMC': {'nom_rapport': 'EVIOSYS PACKAGING', 'alternatives': ['eviosys packaging', 'seme']},
            'SIVC': {'nom_rapport': 'AIR LIQUIDE CI', 'alternatives': ['air liquide ci']},
            'STAC': {'nom_rapport': 'SETAO CI', 'alternatives': ['setao ci']},
            'CIEC': {'nom_rapport': 'CIE CI', 'alternatives': ['cie ci']},
            'SDCC': {'nom_rapport': 'SODE CI', 'alternatives': ['sode ci', 'sode']},
        }
        self.db_conn = None
        self.driver = None
        self.gemini_model = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        self.api_keys = []
        self.current_key_index = 0

    def connect_to_db(self):
        try:
            self.db_conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            logging.info("✅ Connexion DB pour analyse fondamentale réussie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur de connexion DB: {e}")
            return False

    def _load_analysis_memory_from_db(self):
        logging.info("Chargement de la mémoire d'analyse depuis PostgreSQL...")
        try:
            cur = self.db_conn.cursor()
            cur.execute("SELECT report_url FROM fundamental_analysis;")
            self.analysis_memory = {row[0] for row in cur.fetchall()}
            cur.close()
            logging.info(f"{len(self.analysis_memory)} analyses pré-existantes chargées.")
        except Exception as e:
            logging.error(f"❌ Impossible de charger la mémoire d'analyse: {e}")

    def _save_to_memory_db(self, company_id, report, summary):
        try:
            cur = self.db_conn.cursor()
            cur.execute("""
                INSERT INTO fundamental_analysis (company_id, report_url, report_title, report_date, analysis_summary)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (report_url) DO NOTHING;
            """, (company_id, report['url'], report['titre'], report['date'], summary))
            self.db_conn.commit()
            cur.close()
            self.analysis_memory.add(report['url'])
            logging.info(f"    -> Analyse pour {os.path.basename(report['url'])} sauvegardée en DB.")
        except Exception as e:
            logging.error(f"    -> ERREUR lors de la sauvegarde en DB : {e}")
            self.db_conn.rollback()

    def _configure_gemini_with_rotation(self):
        for i in range(1, 20): 
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key: self.api_keys.append(key)
        if not self.api_keys:
            logging.error("❌ Aucune clé API trouvée.")
            return False
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées.")
        return self._rotate_api_key(initial=True)

    def _rotate_api_key(self, initial=False):
        if not initial: self.current_key_index += 1
        if self.current_key_index >= len(self.api_keys):
            logging.error("❌ Toutes les clés API Gemini ont été épuisées.")
            return False
        if not initial: logging.warning(f"Passage à la clé API Gemini #{self.current_key_index + 1}...")
        try:
            genai.configure(api_key=self.api_keys[self.current_key_index])
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash-latest')
            logging.info(f"API Gemini configurée avec la clé #{self.current_key_index + 1}.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur de configuration avec la clé #{self.current_key_index + 1}: {e}")
            return self._rotate_api_key()

    def _analyze_pdf_with_gemini(self, company_id, symbol, report):
        pdf_url = report['url']
        if pdf_url in self.analysis_memory:
            return

        for attempt in range(len(self.api_keys)):
            temp_pdf_path = "temp_report.pdf"
            uploaded_file = None
            try:
                logging.info(f"    -> Nouvelle analyse IA (clé #{self.current_key_index + 1}) : {os.path.basename(pdf_url)}")
                response = self.session.get(pdf_url, timeout=45, verify=False)
                response.raise_for_status()
                with open(temp_pdf_path, 'wb') as f: f.write(response.content)
                
                uploaded_file = genai.upload_file(path=temp_pdf_path, display_name="Rapport Financier")
                
                prompt = """
                Tu es un analyste financier expert spécialisé dans les entreprises de la zone UEMOA cotées à la BRVM.
                Analyse le document PDF ci-joint, qui est un rapport financier, et fournis une synthèse concise en français, structurée en points clés.
                Concentre-toi impérativement sur les aspects suivants :
                - **Évolution du Chiffre d'Affaires (CA)** : Indique la variation en pourcentage et en valeur si possible. Mentionne les raisons de cette évolution.
                - **Évolution du Résultat Net (RN)** : Indique la variation et les facteurs qui l'ont influencée.
                - **Politique de Dividende** : Cherche toute mention de dividende proposé, payé ou des perspectives de distribution.
                - **Performance des Activités Ordinaires/d'Exploitation** : Commente l'évolution de la rentabilité opérationnelle.
                - **Perspectives et Points de Vigilance** : Relève tout point crucial pour un investisseur (endettement, investissements majeurs, perspectives, etc.).
                Si une information n'est pas trouvée, mentionne-le clairement (ex: "Politique de dividende non mentionnée"). Sois factuel et base tes conclusions uniquement sur le document.
                """
                
                response = self.gemini_model.generate_content([prompt, uploaded_file])
                analysis_text = response.text if hasattr(response, 'text') else "Analyse non générée."

                if "erreur" not in analysis_text.lower():
                    self._save_to_memory_db(company_id, report, analysis_text)
                    self.newly_analyzed_reports.append(f"Rapport pour {symbol}:\n{analysis_text}\n")
                return
            except api_exceptions.ResourceExhausted as e:
                logging.warning(f"Quota atteint pour la clé API #{self.current_key_index + 1}.")
                if not self._rotate_api_key(): return
            except Exception as e:
                logging.error(f"    -> Erreur technique inattendue lors de l'analyse IA : {e}")
                return
            finally:
                if uploaded_file:
                    try: genai.delete_file(uploaded_file.name)
                    except: pass
                if os.path.exists(temp_pdf_path): os.remove(temp_pdf_path)

    def setup_selenium(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logging.info("✅ Pilote Selenium (Chrome) démarré.")
        except Exception as e:
            logging.error(f"❌ Impossible de démarrer le pilote Selenium: {e}")
            self.driver = None

    def _normalize_text(self, text):
        if not text: return ""
        text = text.replace('-', ' ')
        text = ''.join(c for c in unicodedata.normalize('NFD', str(text).lower()) if unicodedata.category(c) != 'Mn')
        text = re.sub(r'[^a-z0-9\s\.]', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def _get_symbol_from_name(self, company_name_normalized):
        for symbol, info in self.societes_mapping.items():
            if symbol in self.company_ids:
                for alt in info['alternatives']:
                    if alt in company_name_normalized: return symbol
        return None

    def _extract_date_from_text(self, text):
        if not text: return datetime(1900, 1, 1).date()
        year_match = re.search(r'\b(20\d{2})\b', text)
        if not year_match: return datetime(1900, 1, 1).date()
        year = int(year_match.group(1))
        text_lower = text.lower()
        if 't1' in text_lower or '1er trimestre' in text_lower: return datetime(year, 3, 31).date()
        if 's1' in text_lower or '1er semestre' in text_lower: return datetime(year, 6, 30).date()
        if 't3' in text_lower or '3eme trimestre' in text_lower: return datetime(year, 9, 30).date()
        if 'annuel' in text_lower or '31/12' in text or '31 dec' in text_lower: return datetime(year, 12, 31).date()
        return datetime(year, 6, 15).date()

    def _find_all_reports(self):
        if not self.driver: return {}
        base_url = "https://www.brvm.org/fr/rapports-societes-cotees"
        all_reports = defaultdict(list)
        company_links = []
        try:
            for page_num in range(5): 
                page_url = f"{base_url}?page={page_num}"
                logging.info(f"Navigation vers la page de liste : {page_url}")
                self.driver.get(page_url)
                try:
                    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.views-table")))
                except TimeoutException:
                    logging.info(f"La page {page_num} ne semble pas contenir de tableau. Fin de la pagination.")
                    break
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                table_rows = soup.select("table.views-table tbody tr")
                if not table_rows:
                    logging.info(f"Aucune société trouvée sur la page {page_num}. Fin de la pagination.")
                    break
                for row in table_rows:
                    link_tag = row.find('a', href=True)
                    if link_tag:
                        company_name_normalized = self._normalize_text(link_tag.text)
                        company_url = f"https://www.brvm.org{link_tag['href']}"
                        symbol = self._get_symbol_from_name(company_name_normalized)
                        if symbol:
                            if not any(c['url'] == company_url for c in company_links):
                                company_links.append({'symbol': symbol, 'url': company_url})
                time.sleep(1)

            logging.info(f"Collecte des liens terminée. {len(company_links)} pages de sociétés pertinentes à visiter.")
            for company in company_links:
                symbol = company['symbol']
                logging.info(f"--- Collecte des rapports pour {symbol} ---")
                try:
                    self.driver.get(company['url'])
                    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.views-table")))
                    page_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    report_items = page_soup.select("table.views-table tbody tr")
                    if not report_items:
                        logging.warning(f"  -> Aucun rapport listé sur la page de {symbol}.")
                        continue
                    for item in report_items:
                        pdf_link_tag = item.find('a', href=lambda href: href and '.pdf' in href.lower())
                        if pdf_link_tag:
                            full_url = pdf_link_tag['href'] if pdf_link_tag['href'].startswith('http') else f"https://www.brvm.org{pdf_link_tag['href']}"
                            if not any(r['url'] == full_url for r in all_reports[symbol]):
                                report_data = {
                                    'titre': " ".join(item.get_text().split()),
                                    'url': full_url,
                                    'date': self._extract_date_from_text(item.get_text())
                                }
                                all_reports[symbol].append(report_data)
                    time.sleep(1)
                except TimeoutException:
                    logging.error(f"  -> Timeout sur la page de {symbol}. Passage au suivant.")
                except Exception as e:
                    logging.error(f"  -> Erreur sur la page de {symbol}: {e}. Passage au suivant.")
        except Exception as e:
            logging.error(f"Erreur critique lors du scraping : {e}", exc_info=True)
        return all_reports

    def run_and_get_results(self):
        logging.info("="*60)
        logging.info("ÉTAPE 3 : DÉMARRAGE DE L'ANALYSE FONDAMENTALE (VERSION POSTGRESQL)")
        logging.info("="*60)
        
        if not self.connect_to_db() or not self._configure_gemini_with_rotation():
            if self.db_conn: self.db_conn.close()
            return {}, []
        
        try:
            self._load_analysis_memory_from_db()
            self.setup_selenium()
            if not self.driver: return {}, []

            cur = self.db_conn.cursor()
            cur.execute("SELECT symbol, id, name FROM companies")
            companies_from_db = cur.fetchall()
            cur.close()
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}

            all_reports = self._find_all_reports()
            
            for symbol, (company_id, company_name) in self.company_ids.items():
                logging.info(f"\n📊 Traitement des rapports pour {symbol} - {company_name}")
                company_reports = all_reports.get(symbol, [])
                if not company_reports:
                    logging.info(f"  -> Aucun rapport trouvé sur le site pour {symbol}.")
                    continue

                for report in company_reports:
                    self._analyze_pdf_with_gemini(company_id, symbol, report)
                    time.sleep(1)

            logging.info("\n✅ Traitement de toutes les sociétés terminé.")
            
            cur = self.db_conn.cursor()
            cur.execute("SELECT c.symbol, fa.analysis_summary, c.name FROM fundamental_analysis fa JOIN companies c ON fa.company_id = c.id;")
            final_results = defaultdict(lambda: {'rapports_analyses': [], 'nom': ''})
            for symbol, summary, name in cur.fetchall():
                final_results[symbol]['rapports_analyses'].append({'analyse_ia': summary})
                final_results[symbol]['nom'] = name
            cur.close()
            
            return (dict(final_results), self.newly_analyzed_reports)
            
        except Exception as e:
            logging.critical(f"❌ Erreur critique : {e}", exc_info=True)
            return {}, []
        finally:
            if self.driver: self.driver.quit()
            if self.db_conn: self.db_conn.close()

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
