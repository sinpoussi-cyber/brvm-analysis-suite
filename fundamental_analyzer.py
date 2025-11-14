# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER V21.0 - GEMINI-PRO (SOLUTION STABLE)
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
import base64
from collections import defaultdict
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
import psycopg2

from api_key_manager import APIKeyManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ SOLUTION: Utilisation du modèle stable et universel gemini-pro
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-pro")


class BRVMAnalyzer:
    def __init__(self):
        self.societes_mapping = {
            'NTLC': {'nom_rapport': 'NESTLE CI', 'alternatives': ['nestle ci', 'nestle']},
            'PALC': {'nom_rapport': 'PALM CI', 'alternatives': ['palm ci', 'palmci']},
            'UNLC': {'nom_rapport': 'UNILEVER CI', 'alternatives': ['unilever ci', 'unilever']},
            'SLBC': {'nom_rapport': 'SOLIBRA', 'alternatives': ['solibra ci', 'solibra']},
            'SICC': {'nom_rapport': 'SICOR', 'alternatives': ['sicor ci', 'sicor']},
            'SPHC': {'nom_rapport': 'SAPH', 'alternatives': ['saph ci', 'saph']},
            'SCRC': {'nom_rapport': 'SUCRIVOIRE', 'alternatives': ['sucrivoire', 'sucre']},
            'STBC': {'nom_rapport': 'SITAB', 'alternatives': ['sitab ci', 'sitab']},
            'SGBC': {'nom_rapport': 'SOCIETE GENERALE', 'alternatives': ['sgci', 'societe generale ci']},
            'BICC': {'nom_rapport': 'BICI', 'alternatives': ['bici ci', 'bici cote']},
            'NSBC': {'nom_rapport': 'NSIA BANQUE', 'alternatives': ['nsia ci', 'nsia banque ci']},
            'ECOC': {'nom_rapport': 'ECOBANK CI', 'alternatives': ['ecobank cote', 'eco ci']},
            'BOAC': {'nom_rapport': 'BANK OF AFRICA CI', 'alternatives': ['boa ci', 'boa cote']},
            'SIBC': {'nom_rapport': 'SIB', 'alternatives': ['sib ci', 'societe ivoirienne']},
            'BOABF': {'nom_rapport': 'BANK OF AFRICA BF', 'alternatives': ['boa bf', 'boa burkina']},
            'BOAS': {'nom_rapport': 'BANK OF AFRICA SN', 'alternatives': ['boa sn', 'boa senegal']},
            'BOAM': {'nom_rapport': 'BANK OF AFRICA MALI', 'alternatives': ['boa ml', 'boa mali']},
            'BOAN': {'nom_rapport': 'BANK OF AFRICA NIGER', 'alternatives': ['boa ng', 'boa niger']},
            'BOAB': {'nom_rapport': 'BANK OF AFRICA BENIN', 'alternatives': ['boa bn', 'boa benin']},
            'BICB': {'nom_rapport': 'BICI BENIN', 'alternatives': ['bici bn', 'bici benin']},
            'CBIBF': {'nom_rapport': 'CORIS BANK', 'alternatives': ['coris banking', 'coris bf']},
            'ETIT': {'nom_rapport': 'ECOBANK ETI', 'alternatives': ['eti', 'ecobank transnational']},
            'ORGT': {'nom_rapport': 'ORAGROUP', 'alternatives': ['oragroup togo', 'ora tg']},
            'SAFC': {'nom_rapport': 'SAFCA', 'alternatives': ['safca ci', 'saf ci']},
            'SOGC': {'nom_rapport': 'SOGB', 'alternatives': ['sogb ci', 'societe generale burkina']},
            'SNTS': {'nom_rapport': 'SONATEL', 'alternatives': ['sonatel sn', 'orange senegal']},
            'ORAC': {'nom_rapport': 'ORANGE CI', 'alternatives': ['orange cote', 'oci']},
            'ONTBF': {'nom_rapport': 'ONATEL', 'alternatives': ['onatel bf', 'onatel burkina']},
            'TTLC': {'nom_rapport': 'TOTAL CI', 'alternatives': ['totalenergies ci', 'total cote']},
            'TTLS': {'nom_rapport': 'TOTAL SN', 'alternatives': ['totalenergies sn', 'total senegal']},
            'SHEC': {'nom_rapport': 'VIVO ENERGY', 'alternatives': ['shell ci', 'vivo ci']},
            'CIEC': {'nom_rapport': 'CIE', 'alternatives': ['cie ci', 'compagnie ivoirienne']},
            'CFAC': {'nom_rapport': 'CFAO MOTORS', 'alternatives': ['cfao ci', 'cfao']},
            'PRSC': {'nom_rapport': 'TRACTAFRIC', 'alternatives': ['tractafric motors', 'tractafric ci']},
            'SDSC': {'nom_rapport': 'BOLLORE', 'alternatives': ['africa global logistics', 'sdv ci']},
            'ABJC': {'nom_rapport': 'SERVAIR', 'alternatives': ['servair abidjan', 'servair ci']},
            'BNBC': {'nom_rapport': 'BERNABE', 'alternatives': ['bernabe ci']},
            'NEIC': {'nom_rapport': 'NEI-CEDA', 'alternatives': ['nei ceda', 'neiceda']},
            'UNXC': {'nom_rapport': 'UNIWAX', 'alternatives': ['uniwax ci']},
            'LNBB': {'nom_rapport': 'LOTERIE BENIN', 'alternatives': ['loterie nationale benin']},
            'CABC': {'nom_rapport': 'SICABLE', 'alternatives': ['sicable ci']},
            'FTSC': {'nom_rapport': 'FILTISAC', 'alternatives': ['filtisac ci']},
            'SDCC': {'nom_rapport': 'SODE', 'alternatives': ['sode ci']},
            'SEMC': {'nom_rapport': 'EVIOSYS', 'alternatives': ['crown siem', 'eviosys packaging']},
            'SIVC': {'nom_rapport': 'AIR LIQUIDE', 'alternatives': ['air liquide ci']},
            'STAC': {'nom_rapport': 'SETAO', 'alternatives': ['setao ci']},
            'SMBC': {'nom_rapport': 'SMB', 'alternatives': ['smb ci', 'societe miniere']}
        }
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        
        self.api_manager = APIKeyManager('fundamental_analyzer')

    def connect_to_db(self):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, 
                host=DB_HOST, port=DB_PORT, connect_timeout=10
            )
            return conn
        except Exception as e:
            logging.error(f"❌ Erreur connexion DB: {e}")
            return None

    def _load_analysis_memory_from_db(self):
        logging.info("📂 Chargement mémoire depuis PostgreSQL...")
        conn = self.connect_to_db()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT report_url FROM fundamental_analysis;")
                self.analysis_memory = {row[0] for row in cur.fetchall()}
            logging.info(f"   ✅ {len(self.analysis_memory)} analyse(s) chargée(s)")
        except Exception as e:
            logging.error(f"❌ Erreur chargement mémoire: {e}")
            self.analysis_memory = set()
        finally:
            if conn: conn.close()

    def _save_to_db(self, company_id, report, summary):
        conn = self.connect_to_db()
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fundamental_analysis (company_id, report_url, report_title, report_date, analysis_summary)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (report_url) DO UPDATE SET analysis_summary = EXCLUDED.analysis_summary, updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                """, (company_id, report['url'], report['titre'], report['date'], summary))
                inserted_id = cur.fetchone()[0]
                conn.commit()
            self.analysis_memory.add(report['url'])
            logging.info(f"    ✅ Sauvegardé (ID: {inserted_id})")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur sauvegarde: {e}")
            conn.rollback()
            return False
        finally:
            if conn: conn.close()

    def setup_selenium(self):
        try:
            logging.info("🌐 Configuration Selenium...")
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logging.info("   ✅ Selenium configuré")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur Selenium: {e}")
            self.driver = None
            return False

    def _normalize_text(self, text):
        if not text: return ""
        text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return ' '.join(text.lower().split())

    def _find_all_reports(self):
        all_reports = defaultdict(list)
        try:
            url = "https://www.brvm.org/fr/capitalisation-marche"
            logging.info(f"   🔍 Accès à {url}")
            self.driver.get(url)
            time.sleep(3)
            
            company_links = list(set([elem.get_attribute('href') for elem in self.driver.find_elements(By.TAG_NAME, 'a') if elem.get_attribute('href') and '/societe/' in elem.get_attribute('href')]))
            logging.info(f"   📊 {len(company_links)} page(s) trouvée(s)")
            
            for idx, link in enumerate(company_links, 1):
                try:
                    logging.info(f"   📄 Page {idx}/{len(company_links)}")
                    self.driver.get(link)
                    time.sleep(2)
                    
                    for elem in self.driver.find_elements(By.TAG_NAME, 'a'):
                        href = elem.get_attribute('href')
                        text = elem.text.strip()
                        if not href or not href.endswith('.pdf') or not any(kw in text.lower() for kw in ['rapport', 'financier', 'annuel', 'semestriel']):
                            continue
                        
                        date_match = re.search(r'(20\d{2})', text)
                        report_date = datetime(int(date_match.group(1)), 12, 31).date() if date_match else datetime.now().date()
                        
                        text_norm = self._normalize_text(text)
                        for symbol, info in self.societes_mapping.items():
                            nom = self._normalize_text(info['nom_rapport'])
                            alts = [self._normalize_text(a) for a in info.get('alternatives', [])]
                            if nom in text_norm or any(a in text_norm for a in alts):
                                all_reports[symbol].append({'url': href, 'titre': text, 'date': report_date})
                                break
                except (TimeoutException, WebDriverException, Exception) as e:
                    logging.warning(f"   ⚠️  Erreur page {idx}: {e}")
                    continue
            
            logging.info(f"   ✅ {sum(len(r) for r in all_reports.values())} rapport(s) trouvé(s)")
            return all_reports
        except Exception as e:
            logging.error(f"❌ Erreur recherche: {e}")
            return {}

    def _analyze_pdf_with_gemini(self, company_id, symbol, report, attempt=1, max_attempts=3):
        if report['url'] in self.analysis_memory:
            logging.info(f"    ⏭️  Déjà analysé")
            return None
        
        if attempt == 1:
            logging.info(f"    🆕 NOUVEAU: {os.path.basename(report['url'])}")
        else:
            logging.info(f"    🔄 Tentative {attempt}/{max_attempts}")
        
        prompt = f"""Tu es un analyste financier expert de la BRVM. 
Basé UNIQUEMENT sur le titre du rapport suivant pour la société {symbol}: "{report['titre']}", et sur ta connaissance générale du marché, fournis une analyse hypothétique concise en français.

Structure ta réponse comme suit :
- **Chiffre d'Affaires** : (ex: "Probable croissance/baisse...")
- **Résultat Net** : (ex: "Attendu en hausse/baisse...")
- **Dividendes** : (ex: "La politique de dividende pourrait être maintenue/ajustée...")
- **Perspectives** : (ex: "Les perspectives dépendront du secteur...")

IMPORTANT: Commence ta réponse par "Analyse basée sur le titre du rapport." car tu n'as pas accès au contenu du document."""
        
        api_key = self.api_manager.get_api_key()
        if not api_key:
            logging.error(f"    ❌ Aucune clé Gemini disponible")
            return False
        
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
        headers = {'Content-Type': 'application/json', 'x-goog-api-key': api_key}
        request_body = {"contents": [{"parts": [{"text": prompt}]}]}
        
        try:
            response = requests.post(api_url, headers=headers, json=request_body, timeout=60)
            self.api_manager.record_request()
            
            if response.status_code == 200:
                data = response.json()
                if 'candidates' in data and data['candidates']:
                    analysis_text = data['candidates'][0]['content']['parts'][0]['text']
                    if self._save_to_db(company_id, report, analysis_text):
                        self.newly_analyzed_reports.append(f"Rapport {symbol}:\n{analysis_text}\n")
                        logging.info(f"    ✅ {symbol}: Analyse générée")
                        return True
                logging.warning(f"    ⚠️  Réponse Gemini malformée")
                return False
            
            elif response.status_code == 429:
                logging.warning(f"    ⚠️  Rate limit pour {symbol} (tentative {attempt}/{max_attempts})")
                if attempt < max_attempts and self.api_manager.handle_rate_limit_response():
                    time.sleep(2)
                    return self._analyze_pdf_with_gemini(company_id, symbol, report, attempt + 1, max_attempts)
                else:
                    logging.error(f"    ❌ {symbol}: Échec après {attempt} tentatives.")
                    fallback_text = f"Analyse automatique indisponible (quota API) pour le rapport: {report['titre']}"
                    self._save_to_db(company_id, report, fallback_text)
                    return False
            else:
                logging.error(f"    ❌ Erreur {response.status_code}: {response.text[:200]}")
                return False
        except (requests.exceptions.Timeout, Exception) as e:
            logging.error(f"    ❌ Exception: {e}")
            return False

    def run_and_get_results(self):
        logging.info("="*80)
        logging.info(f"📄 ÉTAPE 4: ANALYSE FONDAMENTALE (V21.0 - {GEMINI_MODEL})")
        logging.info("="*80)
        
        stats = self.api_manager.get_statistics()
        logging.info(f"📊 Clés Gemini: {stats['available']}/{stats['total']} disponible(s)")
        
        self._load_analysis_memory_from_db()
        if not self.setup_selenium(): return {}, []
        
        conn = self.connect_to_db()
        if not conn: return {}, []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, id, name FROM companies")
                self.company_ids = {symbol: (id, name) for symbol, id, name in cur.fetchall()}
        finally:
            conn.close()
            
        all_reports = self._find_all_reports()
        logging.info(f"\n🤖 Phase 2: Analyse IA ({GEMINI_MODEL})...")
        
        total_analyzed = 0
        total_skipped = 0
        
        for symbol, (company_id, company_name) in self.company_ids.items():
            logging.info(f"\n📊 {symbol} - {company_name}")
            company_reports = all_reports.get(symbol, [])
            if not company_reports:
                logging.info(f"   ⏭️  Aucun rapport")
                continue
            
            recent_reports = sorted([r for r in company_reports if r['date'].year >= datetime.now().year - 1], key=lambda x: x['date'], reverse=True)
            new_reports = [r for r in recent_reports if r['url'] not in self.analysis_memory]
            
            logging.info(f"   📂 {len(recent_reports)} rapport(s) récent(s), dont {len(new_reports)} nouveau(x)")
            
            for report in new_reports:
                if self._analyze_pdf_with_gemini(company_id, symbol, report):
                    total_analyzed += 1
            total_skipped += len(recent_reports) - len(new_reports)

        final_stats = self.api_manager.get_statistics()
        logging.info(f"\n✅ Traitement terminé. Nouvelles analyses: {total_analyzed}. Rapports ignorés: {total_skipped}. Requêtes: {final_stats['used_by_module']}.")
        
        conn = self.connect_to_db()
        if not conn: return {}, []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT c.symbol, fa.analysis_summary, c.name FROM fundamental_analysis fa JOIN companies c ON fa.company_id = c.id")
                final_results = defaultdict(lambda: {'rapports_analyses': [], 'nom': ''})
                for symbol, summary, name in cur.fetchall():
                    final_results[symbol]['rapports_analyses'].append({'analyse_ia': summary})
                    final_results[symbol]['nom'] = name
            return dict(final_results), self.newly_analyzed_reports
        finally:
            if self.driver: self.driver.quit()
            if conn: conn.close()

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
