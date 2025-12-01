# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER V27.0 - LIENS DIRECTS (MISTRAL AI)
# ==============================================================================
# Améliorations V27.0:
# - Utilisation des liens directs pour chaque société (plus de scraping générique)
# - Extraction optimisée des rapports financiers
# - Analyse uniquement des nouveaux rapports (non présents en base)
# - Logs détaillés par société
# - Meilleure gestion des erreurs
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION MISTRAL AI
MISTRAL_API_KEY = os.environ.get('MISTRAL_API_KEY')
MISTRAL_MODEL = "mistral-large-latest"
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"

# ✅ OPTIONS
MIN_YEAR = int(os.environ.get('MIN_YEAR', '2015'))  # Année minimale pour filtrer


class BRVMAnalyzer:
    def __init__(self):
        # ✅ LIENS DIRECTS DES SOCIÉTÉS COTÉES (depuis le document Word)
        self.societes_links = {
            'ABJC': {'name': 'SERVAIR ABIDJAN CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/servair-abidjan-ci'},
            'BICB': {'name': 'BIIC BN', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/biic'},
            'BICC': {'name': 'BICI CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bici-ci'},
            'BNBC': {'name': 'BERNABE CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bernabe-ci'},
            'BOAB': {'name': 'BANK OF AFRICA BN', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bank-africa-bn'},
            'BOABF': {'name': 'BANK OF AFRICA BF', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bank-africa-bf'},
            'BOAC': {'name': 'BANK OF AFRICA CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bank-africa-ci'},
            'BOAM': {'name': 'BANK OF AFRICA ML', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bank-africa-ml'},
            'BOAN': {'name': 'BANK OF AFRICA NG', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bank-africa-ng'},
            'BOAS': {'name': 'BANK OF AFRICA SENEGAL', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bank-africa-sn'},
            'CABC': {'name': 'SICABLE CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sicable'},
            'CBIBF': {'name': 'CORIS BANK INTERNATIONAL', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/coris-bank-international'},
            'CFAC': {'name': 'CFAO MOTORS CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/cfao-motors-ci'},
            'CIEC': {'name': 'CIE CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/cie-ci'},
            'ECOC': {'name': "ECOBANK COTE D'IVOIRE", 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/ecobank-ci'},
            'ETIT': {'name': 'ECOBANK TRANS. INCORP. TG', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/ecobank-tg'},
            'FTSC': {'name': 'FILTISAC CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/filtisac-ci'},
            'LNBB': {'name': 'LOTERIE NATIONALE DU BENIN', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/lnb'},
            'NEIC': {'name': 'NEI-CEDA CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/nei-ceda-ci'},
            'NSBC': {'name': "NSIA BANQUE COTE D'IVOIRE", 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/nsbc'},
            'NTLC': {'name': 'NESTLE CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/nestle-ci'},
            'ONTBF': {'name': 'ONATEL BF', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/onatel-bf'},
            'ORAC': {'name': "ORANGE COTE D'IVOIRE", 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/orange-ci'},
            'ORGT': {'name': 'ORAGROUP TOGO', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/oragroup'},
            'PALC': {'name': 'PALM CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/palm-ci'},
            'PRSC': {'name': 'TRACTAFRIC MOTORS CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/tractafric-ci'},
            'SAFC': {'name': 'SAFCA CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/safca-ci'},
            'SCRC': {'name': 'SUCRIVOIRE', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sucrivoire'},
            'SDCC': {'name': 'SODE CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sodeci'},
            'SDSC': {'name': 'AFRICA GLOBAL LOGISTICS CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/bollore-transport-logistics'},
            'SEMC': {'name': 'CROWN SIEM CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/crown-siem-ci'},  # Manquant dans le doc, ajouté
            'SGBC': {'name': "SOCIETE GENERALE COTE D'IVOIRE", 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sgb-ci'},
            'SHEC': {'name': 'VIVO ENERGY CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/vivo-energy-ci'},
            'SIBC': {'name': 'SOCIETE IVOIRIENNE DE BANQUE', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sib'},
            'SICC': {'name': 'SICOR CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sicor'},
            'SIVC': {'name': 'AIR LIQUIDE CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/air-liquide-ci'},
            'SLBC': {'name': 'SOLIBRA CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/solibra'},
            'SMBC': {'name': 'SMB CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/smb'},
            'SNTS': {'name': 'SONATEL SN', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sonatel'},
            'SOGC': {'name': 'SOGB CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sgb-ci'},
            'SPHC': {'name': 'SAPH CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/saph-ci'},
            'STAC': {'name': 'SETAO CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/setao-ci'},
            'STBC': {'name': 'SITAB CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/sitab'},
            'TTLC': {'name': 'TOTALENERGIES MARKETING CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/total'},
            'TTLS': {'name': 'TOTALENERGIES MARKETING SN', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/ttls'},
            'UNLC': {'name': 'UNILEVER CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/unilever-ci'},
            'UNXC': {'name': 'UNIWAX CI', 'url': 'https://www.brvm.org/fr/rapports-societe-cotes/uniwax-ci'}
        }
        
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        self.request_count = 0
        
        # ✅ COMPTEURS DÉTAILLÉS
        self.stats = {
            'reports_found': 0,
            'reports_already_analyzed': 0,
            'reports_to_analyze': 0,
            'reports_analyzed_success': 0,
            'reports_analyzed_failure': 0,
            'api_calls': 0,
            'api_errors': 0,
            'companies_with_reports': 0,
            'companies_without_reports': 0
        }

    def connect_to_db(self):
        """Connexion à PostgreSQL (Supabase)"""
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
        """Charge la mémoire depuis PostgreSQL"""
        logging.info("📂 Chargement mémoire depuis PostgreSQL...")
        conn = self.connect_to_db()
        if not conn: 
            return
        
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT report_url FROM fundamental_analysis;")
                urls = cur.fetchall()
                self.analysis_memory = {row[0] for row in urls}
            
            logging.info(f"   ✅ {len(self.analysis_memory)} analyse(s) en mémoire")
                    
        except Exception as e:
            logging.error(f"❌ Erreur chargement mémoire: {e}")
            self.analysis_memory = set()
        finally:
            if conn: 
                conn.close()

    def _save_to_db(self, company_id, report, summary):
        """Sauvegarde dans PostgreSQL"""
        conn = self.connect_to_db()
        if not conn: 
            return False
        
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
            if conn: 
                conn.close()

    def setup_selenium(self):
        """Configuration Selenium"""
        try:
            logging.info("🌐 Configuration Selenium...")
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
            
            seleniumwire_options = {
                'disable_encoding': True,
                'suppress_connection_errors': True,
                'connection_timeout': 30
            }
            
            self.driver = webdriver.Chrome(
                options=chrome_options,
                seleniumwire_options=seleniumwire_options
            )
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            logging.info("   ✅ Selenium configuré")
            return True
        
        except Exception as e:
            logging.error(f"❌ Erreur Selenium: {e}")
            self.driver = None
            return False

    def _extract_reports_from_page(self, symbol, url):
        """Extrait tous les rapports financiers d'une page société"""
        reports = []
        
        try:
            logging.info(f"   🔍 Accès à {url}")
            self.driver.get(url)
            time.sleep(3)
            
            # Trouver tous les liens PDF
            pdf_links = self.driver.find_elements(By.TAG_NAME, 'a')
            
            for elem in pdf_links:
                try:
                    href = elem.get_attribute('href')
                    text = elem.text.strip()
                    
                    if not href or not href.endswith('.pdf'):
                        continue
                    
                    # Filtrer uniquement les rapports financiers
                    keywords = ['rapport', 'financier', 'annuel', 'semestriel', 'trimestriel', 
                                'etats financiers', 'comptes', 'exercice', 'resultats']
                    
                    if any(kw in text.lower() for kw in keywords):
                        # Extraire l'année
                        date_match = re.search(r'(20\d{2})', text)
                        if date_match:
                            year = int(date_match.group(1))
                            report_date = datetime(year, 12, 31).date()
                        else:
                            report_date = datetime.now().date()
                        
                        # Filtrer par année minimale
                        if report_date.year >= MIN_YEAR:
                            reports.append({
                                'url': href,
                                'titre': text,
                                'date': report_date
                            })
                            self.stats['reports_found'] += 1
                
                except Exception as e:
                    continue
            
            # Trier par date décroissante
            reports.sort(key=lambda x: x['date'], reverse=True)
            
            return reports
            
        except TimeoutException:
            logging.warning(f"   ⏱️  Timeout pour {symbol}")
            return []
        except WebDriverException as e:
            logging.warning(f"   ⚠️  Erreur WebDriver pour {symbol}: {e}")
            return []
        except Exception as e:
            logging.error(f"   ❌ Erreur pour {symbol}: {e}")
            return []

    def _analyze_pdf_with_mistral(self, company_id, symbol, report, attempt=1, max_attempts=3):
        """Analyse un PDF avec Mistral AI"""
        pdf_url = report['url']
        
        # Vérifier si déjà analysé
        if pdf_url in self.analysis_memory:
            logging.info(f"    ⏭️  Déjà analysé")
            self.stats['reports_already_analyzed'] += 1
            return None
        
        # Vérifier en base
        conn = self.connect_to_db()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM fundamental_analysis WHERE report_url = %s;", (pdf_url,))
                    if cur.fetchone():
                        logging.info(f"    ⏭️  Déjà en base")
                        self.analysis_memory.add(pdf_url)
                        self.stats['reports_already_analyzed'] += 1
                        return None
            finally:
                conn.close()
        
        if attempt == 1:
            logging.info(f"    🆕 NOUVEAU: {report['titre'][:60]}...")
            self.stats['reports_to_analyze'] += 1
        else:
            logging.info(f"    🔄 Tentative {attempt}/{max_attempts}")
        
        # Télécharger le PDF
        try:
            pdf_response = self.session.get(pdf_url, timeout=45, verify=False)
            pdf_response.raise_for_status()
            pdf_data = base64.b64encode(pdf_response.content).decode('utf-8')
        except Exception as e:
            logging.error(f"    ❌ Erreur téléchargement PDF: {e}")
            self.stats['reports_analyzed_failure'] += 1
            return False
        
        prompt = """Tu es un analyste financier expert. Analyse ce rapport financier et fournis une synthèse concise en français.

Concentre-toi sur :
- **Chiffre d'Affaires** : Variation en % et valeur
- **Résultat Net** : Évolution et facteurs
- **Dividendes** : Proposé, payé ou perspectives
- **Performance Opérationnelle** : Rentabilité
- **Perspectives** : Points clés

Si une info manque, mentionne-le clairement."""
        
        if not MISTRAL_API_KEY:
            logging.error(f"    ❌ Aucune clé Mistral disponible")
            self.stats['reports_analyzed_failure'] += 1
            return False
        
        # ✅ MISTRAL AI API
        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        request_body = {
            "model": MISTRAL_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": f"data:application/pdf;base64,{pdf_data}"
                        }
                    ]
                }
            ],
            "max_tokens": 2000,
            "temperature": 0.3
        }
        
        try:
            response = requests.post(MISTRAL_API_URL, headers=headers, json=request_body, timeout=120)
            
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                response_json = response.json()
                
                if 'choices' in response_json and len(response_json['choices']) > 0:
                    analysis_text = response_json['choices'][0]['message']['content']
                    
                    if self._save_to_db(company_id, report, analysis_text):
                        self.newly_analyzed_reports.append(f"Rapport {symbol}:\n{analysis_text}\n")
                        logging.info(f"    ✅ {symbol}: Analyse générée")
                        self.stats['reports_analyzed_success'] += 1
                        return True
                
                logging.warning(f"    ⚠️  Réponse Mistral malformée")
                self.stats['api_errors'] += 1
                return False
            
            elif response.status_code == 429:
                logging.warning(f"    ⚠️  Rate limit pour {symbol} (tentative {attempt}/{max_attempts})")
                
                if attempt < max_attempts:
                    time.sleep(10)
                    return self._analyze_pdf_with_mistral(company_id, symbol, report, attempt + 1, max_attempts)
                else:
                    logging.error(f"    ❌ {symbol}: Échec après {attempt} tentatives - FALLBACK")
                    fallback_text = f"Analyse automatique indisponible. Rapport: {report['titre']}"
                    self._save_to_db(company_id, report, fallback_text)
                    self.stats['reports_analyzed_failure'] += 1
                    self.stats['api_errors'] += 1
                    return False
            
            else:
                logging.error(f"    ❌ Erreur {response.status_code}: {response.text[:200]}")
                self.stats['api_errors'] += 1
                self.stats['reports_analyzed_failure'] += 1
                return False
                
        except requests.exceptions.Timeout:
            logging.error(f"    ⏱️  Timeout API Mistral")
            self.stats['api_errors'] += 1
            self.stats['reports_analyzed_failure'] += 1
            return False
        except Exception as e:
            logging.error(f"    ❌ Exception: {e}")
            self.stats['api_errors'] += 1
            self.stats['reports_analyzed_failure'] += 1
            return False

    def run_and_get_results(self):
        """Fonction principale"""
        logging.info("="*80)
        logging.info("📄 ÉTAPE 4: ANALYSE FONDAMENTALE (V27.0 - LIENS DIRECTS)")
        logging.info(f"🤖 Modèle: {MISTRAL_MODEL}")
        logging.info(f"📅 Année minimale: {MIN_YEAR}")
        logging.info(f"📊 Sociétés configurées: {len(self.societes_links)}")
        logging.info("="*80)
        
        conn = None
        try:
            if not MISTRAL_API_KEY:
                logging.error("❌ Clé Mistral non configurée")
                return {}, []
            
            logging.info("✅ Clé Mistral chargée")
            
            # Charger mémoire
            self._load_analysis_memory_from_db()
            
            if not self.setup_selenium():
                logging.error("❌ Impossible d'initialiser Selenium")
                return {}, []
            
            # Récupérer les IDs des sociétés
            conn = self.connect_to_db()
            if not conn: 
                return {}, []
            
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, id, name FROM companies")
                companies_from_db = cur.fetchall()
            conn.close()
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}
            
            logging.info(f"\n🔍 Phase 1: Extraction des rapports (liens directs)...")
            
            # Pour chaque société dans les liens
            for symbol, link_data in sorted(self.societes_links.items()):
                if symbol not in self.company_ids:
                    logging.warning(f"⚠️  {symbol} non trouvé en base")
                    continue
                
                company_id, company_name = self.company_ids[symbol]
                company_url = link_data['url']
                
                logging.info(f"\n📊 {symbol} - {company_name}")
                
                # Extraire les rapports de la page
                reports = self._extract_reports_from_page(symbol, company_url)
                
                if not reports:
                    logging.info(f"   ⏭️  Aucun rapport trouvé (ou tous < {MIN_YEAR})")
                    self.stats['companies_without_reports'] += 1
                    continue
                
                self.stats['companies_with_reports'] += 1
                
                # Afficher les rapports trouvés
                logging.info(f"   📂 {len(reports)} rapport(s) depuis {MIN_YEAR}")
                for report in reports:
                    year = report['date'].year
                    title = report['titre'][:50]
                    if report['url'] in self.analysis_memory:
                        logging.info(f"      ✓ {year} - {title}... (déjà analysé)")
                    else:
                        logging.info(f"      ○ {year} - {title}... (à analyser)")
                
                # Analyser les nouveaux rapports
                new_reports = [r for r in reports if r['url'] not in self.analysis_memory]
                
                for report in new_reports:
                    result = self._analyze_pdf_with_mistral(company_id, symbol, report)
                    if result is False:
                        pass  # Continuer avec les autres
                
                time.sleep(1)  # Pause entre sociétés
            
            # ✅ STATISTIQUES FINALES
            logging.info("\n" + "="*80)
            logging.info("📊 STATISTIQUES DÉTAILLÉES")
            logging.info("="*80)
            logging.info(f"📊 Sociétés avec rapports: {self.stats['companies_with_reports']}")
            logging.info(f"⚠️  Sociétés sans rapports: {self.stats['companies_without_reports']}")
            logging.info(f"📂 Rapports trouvés: {self.stats['reports_found']}")
            logging.info(f"✅ Rapports déjà analysés: {self.stats['reports_already_analyzed']}")
            logging.info(f"🆕 Rapports à analyser: {self.stats['reports_to_analyze']}")
            logging.info(f"✅ Analyses réussies: {self.stats['reports_analyzed_success']}")
            logging.info(f"❌ Analyses échouées: {self.stats['reports_analyzed_failure']}")
            logging.info(f"🔄 Appels API: {self.stats['api_calls']}")
            logging.info(f"⚠️  Erreurs API: {self.stats['api_errors']}")
            
            if self.stats['reports_to_analyze'] > 0:
                success_rate = (self.stats['reports_analyzed_success'] / self.stats['reports_to_analyze'] * 100)
                logging.info(f"📈 Taux de succès: {success_rate:.1f}%")
            
            logging.info("="*80)
            
            # Récupérer résultats finaux
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
            
            logging.info(f"📊 Résultats finaux: {len(final_results)} société(s)")
            return (dict(final_results), self.newly_analyzed_reports)
        
        except Exception as e:
            logging.critical(f"❌ Erreur: {e}", exc_info=True)
            return {}, []
        
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            if conn and not conn.closed: 
                conn.close()


if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
