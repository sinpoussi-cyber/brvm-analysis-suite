# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER V7.2 - SUPABASE + API GEMINI CORRIGÉE
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
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION GEMINI CORRIGÉE - Basé sur vos versions disponibles
GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_API_VERSION = "v2beta"  # Changé pour correspondre à vos versions: v1, v2, v2beta, v2internal, v3, v3beta
REQUESTS_PER_MINUTE_LIMIT = 15

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
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        self.api_keys = []
        self.current_key_index = 0
        self.request_timestamps = []

    def connect_to_db(self):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, 
                host=DB_HOST, port=DB_PORT
            )
            return conn
        except Exception as e:
            logging.error(f"❌ Erreur connexion DB: {e}")
            return None

    def _load_analysis_memory_from_db(self):
        """Charge la mémoire depuis PostgreSQL"""
        logging.info("📂 Chargement mémoire depuis PostgreSQL...")
        conn = self.connect_to_db()
        if not conn: 
            logging.error("❌ Impossible de charger la mémoire: connexion DB échouée")
            return
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'fundamental_analysis'
                    );
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    logging.warning("⚠️  Table 'fundamental_analysis' n'existe pas encore")
                    self.analysis_memory = set()
                    return
                
                cur.execute("SELECT report_url FROM fundamental_analysis;")
                urls = cur.fetchall()
                self.analysis_memory = {row[0] for row in urls}
            
            logging.info(f"   ✅ {len(self.analysis_memory)} analyse(s) chargée(s) depuis DB")
            
            if self.analysis_memory:
                sample_urls = list(self.analysis_memory)[:3]
                logging.info(f"   📋 Exemples d'URLs en mémoire:")
                for url in sample_urls:
                    logging.info(f"      • {url[:80]}...")
                    
        except Exception as e:
            logging.error(f"❌ Erreur chargement mémoire DB: {e}")
            self.analysis_memory = set()
        finally:
            if conn: 
                conn.close()

    def _save_to_db(self, company_id, report, summary):
        """Sauvegarde dans PostgreSQL avec vérification"""
        conn = self.connect_to_db()
        if not conn: 
            logging.error("❌ Impossible de sauvegarder: connexion DB échouée")
            return False
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, analysis_summary FROM fundamental_analysis 
                    WHERE report_url = %s;
                """, (report['url'],))
                existing = cur.fetchone()
                
                if existing:
                    logging.info(f"    ⚠️  Rapport déjà en DB (ID: {existing[0]}), mise à jour...")
                
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
            logging.info(f"    ✅ Sauvegardé dans PostgreSQL (ID: {inserted_id})")
            return True
            
        except Exception as e:
            logging.error(f"❌ Erreur sauvegarde DB: {e}")
            conn.rollback()
            return False
        finally:
            if conn: 
                conn.close()

    def _configure_api_keys(self):
        """Charge les 33 clés API"""
        for i in range(1, 34):  # De 1 à 33
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key: 
                self.api_keys.append(key)
        
        if not self.api_keys:
            logging.error("❌ Aucune clé API trouvée")
            return False
        
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées")
        logging.info(f"📝 Modèle: {GEMINI_MODEL} | API Version: {GEMINI_API_VERSION}")
        return True

    def _analyze_pdf_with_direct_api(self, company_id, symbol, report):
        """Analyse un PDF avec l'API Gemini + vérification mémoire STRICTE"""
        pdf_url = report['url']
        
        # ✅ VÉRIFICATION MÉMOIRE STRICTE - Arrêter immédiatement si déjà analysé
        if pdf_url in self.analysis_memory:
            logging.info(f"    ⏭️  Déjà analysé: {os.path.basename(pdf_url)}")
            return None  # Retourner None pour indiquer qu'on a sauté
        
        # ✅ DOUBLE VÉRIFICATION : Checker aussi dans la DB en temps réel
        conn = self.connect_to_db()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM fundamental_analysis WHERE report_url = %s;", (pdf_url,))
                    if cur.fetchone():
                        logging.info(f"    ⏭️  Déjà en base: {os.path.basename(pdf_url)}")
                        self.analysis_memory.add(pdf_url)  # Ajouter à la mémoire locale
                        return None
            except Exception as e:
                logging.error(f"    ⚠️  Erreur vérification DB: {e}")
            finally:
                conn.close()
        
        # ✅ Le rapport n'a jamais été analysé, on procède
        logging.info(f"    🆕 NOUVEAU rapport à analyser: {os.path.basename(pdf_url)}")
        
        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        if len(self.request_timestamps) >= REQUESTS_PER_MINUTE_LIMIT:
            sleep_time = 60 - (now - self.request_timestamps[0])
            logging.warning(f"⏸️  Pause rate limit: {sleep_time + 1:.1f}s")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []
        
        if self.current_key_index >= len(self.api_keys):
            logging.error("❌ Toutes les clés API épuisées")
            return None
        
        api_key = self.api_keys[self.current_key_index]
        # ✅ URL CORRIGÉE avec v2beta
        api_url = f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models/{GEMINI_MODEL}:generateContent?key={api_key}"
        
        try:
            logging.info(f"    🤖 Analyse IA (clé #{self.current_key_index + 1})")
            
            pdf_response = self.session.get(pdf_url, timeout=45, verify=False)
            pdf_response.raise_for_status()
            pdf_data = base64.b64encode(pdf_response.content).decode('utf-8')
            
            prompt = """Tu es un analyste financier expert spécialisé dans les entreprises de la zone UEMOA cotées à la BRVM.
Analyse le document PDF ci-joint, qui est un rapport financier, et fournis une synthèse concise en français, structurée en points clés.
Concentre-toi impérativement sur les aspects suivants :
- **Évolution du Chiffre d'Affaires (CA)** : Indique la variation en pourcentage et en valeur si possible.
- **Évolution du Résultat Net (RN)** : Indique la variation et les facteurs qui l'ont influencée.
- **Politique de Dividende** : Cherche toute mention de dividende proposé, payé ou des perspectives.
- **Performance des Activités Ordinaires/d'Exploitation** : Commente l'évolution de la rentabilité opérationnelle.
- **Perspectives et Points de Vigilance** : Relève tout point crucial pour un investisseur.
Si une information n'est pas trouvée, mentionne-le clairement. Sois factuel et base tes conclusions uniquement sur le document."""
            
            request_body = {
                "contents": [{
                    "parts": [
                        {"text": prompt}, 
                        {"inline_data": {"mime_type": "application/pdf", "data": pdf_data}}
                    ]
                }],
                "generationConfig": {
                    "temperature": 0.7,
                    "topK": 40,
                    "topP": 0.95,
                    "maxOutputTokens": 2048,
                }
            }
            
            self.request_timestamps.append(time.time())
            response = requests.post(api_url, json=request_body, timeout=120)
            
            if response.status_code == 429:
                logging.warning(f"⚠️  Quota atteint pour clé #{self.current_key_index + 1}")
                self.current_key_index += 1
                return self._analyze_pdf_with_direct_api(company_id, symbol, report)
            
            response.raise_for_status()
            response_json = response.json()
            
            analysis_text = response_json['candidates'][0]['content']['parts'][0]['text']
            
            if "erreur" not in analysis_text.lower():
                if self._save_to_db(company_id, report, analysis_text):
                    self.newly_analyzed_reports.append(f"Rapport pour {symbol}:\n{analysis_text}\n")
                    return True  # Succès
            
            return False
        
        except Exception as e:
            logging.error(f"    ❌ Erreur clé #{self.current_key_index + 1}: {e}")
            self.current_key_index += 1
            if self.current_key_index < len(self.api_keys):
                return self._analyze_pdf_with_direct_api(company_id, symbol, report)
            return False

    def setup_selenium(self):
        """Configuration Selenium"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logging.info("✅ Pilote Selenium démarré")
        except Exception as e:
            logging.error(f"❌ Impossible de démarrer Selenium: {e}")
            self.driver = None

    def _normalize_text(self, text):
        """Normalise le texte pour la comparaison"""
        if not text: 
            return ""
        text = text.replace('-', ' ')
        text = ''.join(c for c in unicodedata.normalize('NFD', str(text).lower()) if unicodedata.category(c) != 'Mn')
        text = re.sub(r'[^a-z0-9\s\.]', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def _get_symbol_from_name(self, company_name_normalized):
        """Trouve le symbole depuis le nom normalisé"""
        for symbol, info in self.societes_mapping.items():
            if symbol in self.company_ids:
                for alt in info['alternatives']:
                    if alt in company_name_normalized: 
                        return symbol
        return None

    def _extract_date_from_text(self, text):
        """Extrait la date depuis le texte"""
        if not text: 
            return datetime(1900, 1, 1).date()
        
        year_match = re.search(r'\b(20\d{2})\b', text)
        if not year_match: 
            return datetime(1900, 1, 1).date()
        
        year = int(year_match.group(1))
        text_lower = text.lower()
        
        if 't1' in text_lower or '1er trimestre' in text_lower: 
            return datetime(year, 3, 31).date()
        if 's1' in text_lower or '1er semestre' in text_lower: 
            return datetime(year, 6, 30).date()
        if 't3' in text_lower or '3eme trimestre' in text_lower: 
            return datetime(year, 9, 30).date()
        if 'annuel' in text_lower: 
            return datetime(year, 12, 31).date()
        
        return datetime(year, 6, 15).date()

    def _find_all_reports(self):
        """Trouve tous les rapports sur le site BRVM"""
        if not self.driver: 
            return {}
        
        base_url = "https://www.brvm.org/fr/rapports-societes-cotees"
        all_reports = defaultdict(list)
        company_links = []
        
        try:
            for page_num in range(5):
                page_url = f"{base_url}?page={page_num}"
                logging.info(f"🔍 Navigation page {page_num}...")
                self.driver.get(page_url)
                
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.views-table"))
                    )
                except TimeoutException:
                    logging.info(f"   ⏭️  Page {page_num} sans tableau, fin pagination")
                    break
                
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                table_rows = soup.select("table.views-table tbody tr")
                
                if not table_rows:
                    logging.info(f"   ⏭️  Aucune société page {page_num}, fin pagination")
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
            
            logging.info(f"📊 {len(company_links)} page(s) de sociétés à visiter")
            
            for company in company_links:
                symbol = company['symbol']
                logging.info(f"--- Collecte rapports pour {symbol} ---")
                
                try:
                    self.driver.get(company['url'])
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.views-table"))
                    )
                    page_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    report_items = page_soup.select("table.views-table tbody tr")
                    
                    if not report_items:
                        logging.warning(f"   ⚠️  Aucun rapport pour {symbol}")
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
                    logging.error(f"   ❌ Timeout {symbol}")
                except Exception as e:
                    logging.error(f"   ❌ Erreur {symbol}: {e}")
        
        except Exception as e:
            logging.error(f"❌ Erreur critique scraping: {e}", exc_info=True)
        
        return all_reports

    def run_and_get_results(self):
        """Fonction principale avec système de mémoire optimisé"""
        logging.info("="*80)
        logging.info("📄 ÉTAPE 4: ANALYSE FONDAMENTALE (V7.3 - MÉMOIRE OPTIMISÉE)")
        logging.info("="*80)
        
        conn = None
        try:
            if not self._configure_api_keys():
                return {}, []
            
            # ✅ CHARGEMENT MÉMOIRE AU DÉMARRAGE
            self._load_analysis_memory_from_db()
            logging.info(f"📊 Mémoire chargée: {len(self.analysis_memory)} rapport(s) déjà analysé(s)")
            
            if len(self.analysis_memory) > 0:
                logging.info(f"   💡 Ces rapports seront automatiquement ignorés")
            else:
                logging.info(f"   💡 Première exécution - tous les rapports seront analysés")
            
            self.setup_selenium()
            if not self.driver: 
                return {}, []
            
            conn = self.connect_to_db()
            if not conn: 
                return {}, []
            
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, id, name FROM companies")
                companies_from_db = cur.fetchall()
            conn.close()
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}
            
            # ✅ COLLECTE DES RAPPORTS
            logging.info("\n🔍 Phase 1: Collecte des rapports sur le site BRVM...")
            all_reports = self._find_all_reports()
            
            # ✅ STATISTIQUES PRÉ-TRAITEMENT
            total_reports_found = sum(len(reports) for reports in all_reports.values())
            logging.info(f"\n📊 Statistiques de collecte:")
            logging.info(f"   • Total rapports trouvés: {total_reports_found}")
            logging.info(f"   • Sociétés avec rapports: {len(all_reports)}")
            
            # ✅ ANALYSE AVEC FILTRAGE INTELLIGENT
            logging.info(f"\n🤖 Phase 2: Analyse des nouveaux rapports...")
            
            total_analyzed = 0
            total_skipped = 0
            
            for symbol, (company_id, company_name) in self.company_ids.items():
                logging.info(f"\n📊 Traitement {symbol} - {company_name}")
                company_reports = all_reports.get(symbol, [])
                
                if not company_reports:
                    logging.info(f"   ⏭️  Aucun rapport pour {symbol}")
                    continue
                
                date_2024_start = datetime(2024, 1, 1).date()
                recent_reports = [r for r in company_reports if r['date'] >= date_2024_start]
                recent_reports.sort(key=lambda x: x['date'], reverse=True)
                
                logging.info(f"   📂 {len(recent_reports)} rapport(s) récent(s) trouvé(s)")
                
                # ✅ FILTRAGE INTELLIGENT : Séparer les rapports déjà analysés des nouveaux
                already_analyzed = []
                new_reports = []
                
                for report in recent_reports:
                    if report['url'] in self.analysis_memory:
                        already_analyzed.append(report)
                    else:
                        new_reports.append(report)
                
                logging.info(f"   ✅ Déjà analysés: {len(already_analyzed)}")
                logging.info(f"   🆕 Nouveaux à analyser: {len(new_reports)}")
                
                # ✅ TRAITER UNIQUEMENT LES NOUVEAUX RAPPORTS
                for report in new_reports:
                    result = self._analyze_pdf_with_direct_api(company_id, symbol, report)
                    if result is True:
                        total_analyzed += 1
                    elif result is None:
                        total_skipped += 1
                
                # Les rapports déjà analysés sont comptés à part
                total_skipped += len(already_analyzed)
            
            logging.info("\n✅ Traitement terminé")
            logging.info(f"📊 Nouvelles analyses effectuées: {total_analyzed}")
            logging.info(f"📊 Rapports ignorés (déjà en DB): {total_skipped}")
            logging.info(f"💾 Total dans la mémoire: {len(self.analysis_memory)} rapport(s)")
            
            conn = self.connect_to_db()
            if not conn: 
                return {}, []
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.symbol, fa.analysis_summary, c.name 
                    FROM fundamental_analysis fa 
                    JOIN companies c ON fa.company_id = c.id
                """)
                final_results = defaultdict(lambda: {'rapports_analyses': [], 'nom': ''})
                
                for symbol, summary, name in cur.fetchall():
                    final_results[symbol]['rapports_analyses'].append({'analyse_ia': summary})
                    final_results[symbol]['nom'] = name
            
            logging.info(f"📊 Résultats finaux: {len(final_results)} société(s) avec analyses")
            return (dict(final_results), self.newly_analyzed_reports)
        
        except Exception as e:
            logging.critical(f"❌ Erreur critique: {e}", exc_info=True)
            return {}, []
        
        finally:
            if self.driver: 
                self.driver.quit()
            if conn and not conn.closed: 
                conn.close()

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
