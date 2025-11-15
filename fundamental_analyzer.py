# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER V24.1 - OPENAI GPT-4o (CORRECTION PROXY)
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
from io import BytesIO
from collections import defaultdict
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
import psycopg2
import pdfplumber
import openai
import httpx # <-- NOUVEL IMPORT

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

OPENAI_MODEL = "gpt-4o"

class BRVMAnalyzer:
    def __init__(self):
        # ... (le dictionnaire societes_mapping reste identique, pas besoin de le copier ici) ...
        self.societes_mapping = {
            'NTLC': {'nom_rapport': 'NESTLE CI', 'alternatives': ['nestle ci', 'nestle']}, 'PALC': {'nom_rapport': 'PALM CI', 'alternatives': ['palm ci', 'palmci']}, 'UNLC': {'nom_rapport': 'UNILEVER CI', 'alternatives': ['unilever ci', 'unilever']}, 'SLBC': {'nom_rapport': 'SOLIBRA', 'alternatives': ['solibra ci', 'solibra']}, 'SICC': {'nom_rapport': 'SICOR', 'alternatives': ['sicor ci', 'sicor']}, 'SPHC': {'nom_rapport': 'SAPH', 'alternatives': ['saph ci', 'saph']}, 'SCRC': {'nom_rapport': 'SUCRIVOIRE', 'alternatives': ['sucrivoire', 'sucre']}, 'STBC': {'nom_rapport': 'SITAB', 'alternatives': ['sitab ci', 'sitab']}, 'SGBC': {'nom_rapport': 'SOCIETE GENERALE', 'alternatives': ['sgci', 'societe generale ci']}, 'BICC': {'nom_rapport': 'BICI', 'alternatives': ['bici ci', 'bici cote']}, 'NSBC': {'nom_rapport': 'NSIA BANQUE', 'alternatives': ['nsia ci', 'nsia banque ci']}, 'ECOC': {'nom_rapport': 'ECOBANK CI', 'alternatives': ['ecobank cote', 'eco ci']}, 'BOAC': {'nom_rapport': 'BANK OF AFRICA CI', 'alternatives': ['boa ci', 'boa cote']}, 'SIBC': {'nom_rapport': 'SIB', 'alternatives': ['sib ci', 'societe ivoirienne']}, 'BOABF': {'nom_rapport': 'BANK OF AFRICA BF', 'alternatives': ['boa bf', 'boa burkina']}, 'BOAS': {'nom_rapport': 'BANK OF AFRICA SN', 'alternatives': ['boa sn', 'boa senegal']}, 'BOAM': {'nom_rapport': 'BANK OF AFRICA MALI', 'alternatives': ['boa ml', 'boa mali']}, 'BOAN': {'nom_rapport': 'BANK OF AFRICA NIGER', 'alternatives': ['boa ng', 'boa niger']}, 'BOAB': {'nom_rapport': 'BANK OF AFRICA BENIN', 'alternatives': ['boa bn', 'boa benin']}, 'BICB': {'nom_rapport': 'BICI BENIN', 'alternatives': ['bici bn', 'bici benin']}, 'CBIBF': {'nom_rapport': 'CORIS BANK', 'alternatives': ['coris banking', 'coris bf']}, 'ETIT': {'nom_rapport': 'ECOBANK ETI', 'alternatives': ['eti', 'ecobank transnational']}, 'ORGT': {'nom_rapport': 'ORAGROUP', 'alternatives': ['oragroup togo', 'ora tg']}, 'SAFC': {'nom_rapport': 'SAFCA', 'alternatives': ['safca ci', 'saf ci']}, 'SOGC': {'nom_rapport': 'SOGB', 'alternatives': ['sogb ci', 'societe generale burkina']}, 'SNTS': {'nom_rapport': 'SONATEL', 'alternatives': ['sonatel sn', 'orange senegal']}, 'ORAC': {'nom_rapport': 'ORANGE CI', 'alternatives': ['orange cote', 'oci']}, 'ONTBF': {'nom_rapport': 'ONATEL', 'alternatives': ['onatel bf', 'onatel burkina']}, 'TTLC': {'nom_rapport': 'TOTAL CI', 'alternatives': ['totalenergies ci', 'total cote']}, 'TTLS': {'nom_rapport': 'TOTAL SN', 'alternatives': ['totalenergies sn', 'total senegal']}, 'SHEC': {'nom_rapport': 'VIVO ENERGY', 'alternatives': ['shell ci', 'vivo ci']}, 'CIEC': {'nom_rapport': 'CIE', 'alternatives': ['cie ci', 'compagnie ivoirienne']}, 'CFAC': {'nom_rapport': 'CFAO MOTORS', 'alternatives': ['cfao ci', 'cfao']}, 'PRSC': {'nom_rapport': 'TRACTAFRIC', 'alternatives': ['tractafric motors', 'tractafric ci']}, 'SDSC': {'nom_rapport': 'BOLLORE', 'alternatives': ['africa global logistics', 'sdv ci']}, 'ABJC': {'nom_rapport': 'SERVAIR', 'alternatives': ['servair abidjan', 'servair ci']}, 'BNBC': {'nom_rapport': 'BERNABE', 'alternatives': ['bernabe ci']}, 'NEIC': {'nom_rapport': 'NEI-CEDA', 'alternatives': ['nei ceda', 'neiceda']}, 'UNXC': {'nom_rapport': 'UNIWAX', 'alternatives': ['uniwax ci']}, 'LNBB': {'nom_rapport': 'LOTERIE BENIN', 'alternatives': ['loterie nationale benin']}, 'CABC': {'nom_rapport': 'SICABLE', 'alternatives': ['sicable ci']}, 'FTSC': {'nom_rapport': 'FILTISAC', 'alternatives': ['filtisac ci']}, 'SDCC': {'nom_rapport': 'SODE', 'alternatives': ['sode ci']}, 'SEMC': {'nom_rapport': 'EVIOSYS', 'alternatives': ['crown siem', 'eviosys packaging']}, 'SIVC': {'nom_rapport': 'AIR LIQUIDE', 'alternatives': ['air liquide ci']}, 'STAC': {'nom_rapport': 'SETAO', 'alternatives': ['setao ci']}, 'SMBC': {'nom_rapport': 'SMB', 'alternatives': ['smb ci', 'societe miniere']}
        }
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        
        # Initialisation du client OpenAI
        try:
            # LIGNE MODIFIÉE : Création d'un client HTTP propre pour éviter le conflit
            clean_http_client = httpx.Client(proxies={})
            self.openai_client = openai.OpenAI(
                api_key=os.environ.get("OPENAI_API_KEY"),
                http_client=clean_http_client # <-- LIGNE MODIFIÉE
            )
            logging.info("✅ Client OpenAI initialisé.")
        except Exception as e:
            self.openai_client = None
            logging.error(f"❌ Erreur initialisation client OpenAI: {e}")

    # Le reste du fichier (connect_to_db, _load_analysis_memory_from_db, etc.) est identique
    # ... Collez le reste du code du fichier fundamental_analyzer.py précédent ici ...
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
                    ON CONFLICT (report_url) DO UPDATE SET
                        analysis_summary = EXCLUDED.analysis_summary,
                        updated_at = CURRENT_TIMESTAMP
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
            company_links = [elem.get_attribute('href') for elem in self.driver.find_elements(By.TAG_NAME, 'a') if elem.get_attribute('href') and '/societe/' in elem.get_attribute('href')]
            company_links = list(set(company_links))
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
                        for symbol, info in self.societes_mapping.items():
                            text_norm = self._normalize_text(text)
                            if self._normalize_text(info['nom_rapport']) in text_norm or any(self._normalize_text(a) in text_norm for a in info.get('alternatives', [])):
                                all_reports[symbol].append({'url': href, 'titre': text, 'date': report_date})
                                break
                except (TimeoutException, WebDriverException) as e:
                    logging.warning(f"   ⏱️  Erreur page {idx}: {e}")
            logging.info(f"   ✅ {sum(len(r) for r in all_reports.values())} rapport(s) trouvé(s)")
            return all_reports
        except Exception as e:
            logging.error(f"❌ Erreur recherche: {e}")
            return {}

    def _analyze_pdf_with_openai(self, company_id, symbol, report):
        pdf_url = report['url']
        if pdf_url in self.analysis_memory:
            logging.info(f"    ⏭️  Déjà analysé")
            return None

        logging.info(f"    🆕 NOUVEAU: {os.path.basename(pdf_url)}")
        
        if not self.openai_client:
            logging.error("    ❌ Client OpenAI non disponible.")
            return False

        try:
            pdf_response = self.session.get(pdf_url, timeout=45, verify=False)
            pdf_response.raise_for_status()
            
            pdf_text = ""
            with pdfplumber.open(BytesIO(pdf_response.content)) as pdf:
                for page in pdf.pages[:20]:
                    pdf_text += page.extract_text() or ""
            
            if not pdf_text.strip():
                logging.warning("    ⚠️  Impossible d'extraire le texte du PDF.")
                return False

            pdf_text = pdf_text[:25000]

            prompt = f"""Tu es un analyste financier expert spécialisé sur le marché de la BRVM. Analyse le contenu textuel suivant, extrait d'un rapport financier, et fournis une synthèse concise en français.

Concentre-toi sur les points suivants :
- **Chiffre d'Affaires** : Quelle est sa valeur et son évolution en pourcentage par rapport à la période précédente ?
- **Résultat Net** : Quelle est sa valeur, son évolution, et quels sont les facteurs clés (positifs ou négatifs) ?
- **Politique de Dividendes** : Un dividende est-il proposé ? Si oui, quel est son montant ?
- **Performance Opérationnelle** : Comment la rentabilité a-t-elle évolué ?
- **Perspectives** : Quelles sont les perspectives d'avenir mentionnées dans le rapport ?

Si une information est manquante, mentionne-le clairement. Structure ta réponse avec des titres clairs.

Voici le texte du rapport à analyser :
---
{pdf_text}
---
"""
            
            response = self.openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "Tu es un analyste financier expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1024
            )
            
            analysis_text = response.choices[0].message.content

            if self._save_to_db(company_id, report, analysis_text):
                self.newly_analyzed_reports.append(f"Rapport {symbol}:\n{analysis_text}\n")
                logging.info(f"    ✅ {symbol}: Analyse OpenAI générée")
                return True
            return False

        except openai.APIError as e:
            logging.error(f"    ❌ Erreur API OpenAI pour {symbol}: {e}")
            return False
        except Exception as e:
            logging.error(f"    ❌ Erreur inattendue pour {symbol}: {e}")
            return False

    def run_and_get_results(self):
        logging.info("="*80)
        logging.info(f"📄 ÉTAPE 4: ANALYSE FONDAMENTALE (V24.1 - OpenAI {OPENAI_MODEL})")
        logging.info("="*80)
        
        if not self.openai_client:
            logging.error("❌ Analyse fondamentale annulée: client OpenAI non initialisé.")
            return {}, []
        
        self._load_analysis_memory_from_db()
        if not self.setup_selenium(): return {}, []
        
        conn = self.connect_to_db()
        if not conn: return {}, []
        
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, id, name FROM companies")
            self.company_ids = {symbol: (id, name) for symbol, id, name in cur.fetchall()}
        conn.close()
        
        logging.info(f"\n🔍 Phase 1: Collecte rapports...")
        all_reports = self._find_all_reports()
        
        logging.info(f"\n🤖 Phase 2: Analyse IA ({OPENAI_MODEL})...")
        total_analyzed, total_skipped = 0, 0
        
        for symbol, (company_id, company_name) in self.company_ids.items():
            logging.info(f"\n📊 {symbol} - {company_name}")
            company_reports = all_reports.get(symbol, [])
            if not company_reports:
                logging.info(f"   ⏭️  Aucun rapport")
                continue
            
            recent = sorted([r for r in company_reports if r['date'].year >= 2023], key=lambda x: x['date'], reverse=True)
            new = [r for r in recent if r['url'] not in self.analysis_memory]
            logging.info(f"   📂 {len(recent)} rapport(s) récent(s), dont {len(new)} nouveau(x)")
            
            for report in new[:2]: 
                result = self._analyze_pdf_with_openai(company_id, symbol, report)
                if result is True: total_analyzed += 1
                elif result is None: total_skipped += 1
        
        logging.info(f"\n✅ Traitement terminé. Nouvelles analyses: {total_analyzed}")
        
        conn = self.connect_to_db()
        if not conn: return {}, []
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.symbol, fa.analysis_summary, c.name 
                FROM fundamental_analysis fa JOIN companies c ON fa.company_id = c.id
            """)
            final_results = defaultdict(lambda: {'rapports_analyses': [], 'nom': ''})
            for symbol, summary, name in cur.fetchall():
                final_results[symbol]['rapports_analyses'].append({'analyse_ia': summary})
                final_results[symbol]['nom'] = name
        
        logging.info(f"📊 Résultats: {len(final_results)} société(s)")
        return (dict(final_results), self.newly_analyzed_reports)
    
    def __del__(self):
        if self.driver:
            self.driver.quit()

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
