# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER V10.0 FINALE - OPTIMISÉ AVEC GESTION D'ERREURS
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
from selenium.common.exceptions import (
    TimeoutException, 
    WebDriverException, 
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2

# Import du gestionnaire de clés API
from api_key_manager import APIKeyManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION GEMINI
GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_API_VERSION = "v1beta"

# Configuration Selenium
SELENIUM_TIMEOUT = 30  # secondes
MAX_SELENIUM_RETRIES = 3
PAGE_LOAD_DELAY = 3  # secondes

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
        
        # ✅ Gestionnaire de clés API
        self.api_manager = APIKeyManager('fundamental_analyzer')
        self.current_api_key = None

    def connect_to_db(self):
        """Connexion à PostgreSQL (Supabase) avec gestion d'erreurs"""
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, 
                host=DB_HOST, port=DB_PORT,
                connect_timeout=10
            )
            return conn
        except psycopg2.OperationalError as e:
            logging.error(f"❌ Erreur connexion DB (timeout ou réseau): {e}")
            return None
        except psycopg2.Error as e:
            logging.error(f"❌ Erreur PostgreSQL: {e}")
            return None
        except Exception as e:
            logging.error(f"❌ Erreur connexion DB inattendue: {e}")
            return None

    def _load_analysis_memory_from_db(self):
        """Charge la mémoire depuis PostgreSQL avec gestion d'erreurs"""
        logging.info("📂 Chargement mémoire depuis PostgreSQL...")
        conn = self.connect_to_db()
        if not conn: 
            logging.error("❌ Impossible de charger la mémoire: connexion DB échouée")
            self.analysis_memory = set()
            return
        
        try:
            with conn.cursor() as cur:
                # Vérifier existence de la table
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'fundamental_analysis'
                    );
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    logging.warning("⚠️  Table 'fundamental_analysis' n'existe pas")
                    self.analysis_memory = set()
                    return
                
                cur.execute("SELECT report_url FROM fundamental_analysis;")
                urls = cur.fetchall()
                self.analysis_memory = {row[0] for row in urls}
            
            logging.info(f"   ✅ {len(self.analysis_memory)} analyse(s) chargée(s)")
                    
        except psycopg2.Error as e:
            logging.error(f"❌ Erreur SQL lors du chargement mémoire: {e}")
            self.analysis_memory = set()
        except Exception as e:
            logging.error(f"❌ Erreur inattendue chargement mémoire: {e}")
            self.analysis_memory = set()
        finally:
            if conn: 
                conn.close()

    def _save_to_db(self, company_id, report, summary):
        """Sauvegarde dans PostgreSQL avec gestion d'erreurs complète"""
        conn = self.connect_to_db()
        if not conn: 
            logging.error("❌ Impossible de sauvegarder: connexion DB échouée")
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
            
        except psycopg2.IntegrityError as e:
            logging.error(f"❌ Erreur intégrité données: {e}")
            conn.rollback()
            return False
        except psycopg2.Error as e:
            logging.error(f"❌ Erreur SQL sauvegarde: {e}")
            conn.rollback()
            return False
        except Exception as e:
            logging.error(f"❌ Erreur inattendue sauvegarde: {e}")
            conn.rollback()
            return False
        finally:
            if conn: 
                conn.close()

    def setup_selenium(self):
        """Configuration Selenium avec gestion d'erreurs robuste"""
        logging.info("🌐 Configuration Selenium...")
        
        retry_count = 0
        while retry_count < MAX_SELENIUM_RETRIES:
            try:
                chrome_options = Options()
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--disable-blink-features=AutomationControlled')
                chrome_options.add_argument('--window-size=1920,1080')
                chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
                
                # Options de performance
                chrome_options.add_argument('--disable-extensions')
                chrome_options.add_argument('--disable-images')
                chrome_options.add_argument('--blink-settings=imagesEnabled=false')
                
                seleniumwire_options = {
                    'verify_ssl': False,
                    'suppress_connection_errors': True,
                    'connection_timeout': 30
                }
                
                self.driver = webdriver.Chrome(
                    options=chrome_options,
                    seleniumwire_options=seleniumwire_options
                )
                self.driver.set_page_load_timeout(SELENIUM_TIMEOUT)
                self.driver.implicitly_wait(10)
                
                logging.info("   ✅ Selenium configuré avec succès")
                return True
            
            except WebDriverException as e:
                retry_count += 1
                logging.error(f"❌ Erreur WebDriver (tentative {retry_count}/{MAX_SELENIUM_RETRIES}): {e}")
                if retry_count < MAX_SELENIUM_RETRIES:
                    time.sleep(5)
                    continue
                else:
                    logging.error("❌ Échec configuration Selenium après plusieurs tentatives")
                    self.driver = None
                    return False
            
            except Exception as e:
                logging.error(f"❌ Erreur inattendue configuration Selenium: {e}")
                self.driver = None
                return False
        
        return False

    def _normalize_text(self, text):
        """Normalise le texte pour comparaison"""
        if not text:
            return ""
        
        try:
            # Supprimer les accents
            text = ''.join(c for c in unicodedata.normalize('NFD', text) 
                           if unicodedata.category(c) != 'Mn')
            
            # Mettre en minuscules et supprimer espaces multiples
            text = ' '.join(text.lower().split())
            
            return text
        except Exception as e:
            logging.warning(f"⚠️  Erreur normalisation texte: {e}")
            return text.lower() if text else ""

    def _safe_get_page(self, url, max_retries=3):
        """Navigation sécurisée vers une page avec retry"""
        if not self.driver:
            logging.error("❌ Driver Selenium non initialisé")
            return False
        
        for attempt in range(max_retries):
            try:
                logging.info(f"   🔍 Accès à {url} (tentative {attempt + 1}/{max_retries})")
                self.driver.get(url)
                time.sleep(PAGE_LOAD_DELAY)
                return True
            
            except TimeoutException:
                logging.warning(f"   ⏱️  Timeout lors du chargement de {url}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return False
            
            except WebDriverException as e:
                logging.error(f"   ❌ Erreur WebDriver sur {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return False
            
            except Exception as e:
                logging.error(f"   ❌ Erreur inattendue navigation vers {url}: {e}")
                return False
        
        return False

    def _safe_find_elements(self, by, value, max_retries=2):
        """Recherche d'éléments avec gestion d'erreurs"""
        if not self.driver:
            return []
        
        for attempt in range(max_retries):
            try:
                elements = self.driver.find_elements(by, value)
                return elements
            
            except StaleElementReferenceException:
                logging.warning(f"   ⚠️  Élément obsolète, retry {attempt + 1}/{max_retries}")
                time.sleep(1)
                continue
            
            except NoSuchElementException:
                logging.debug(f"   ℹ️  Aucun élément trouvé pour {value}")
                return []
            
            except WebDriverException as e:
                logging.error(f"   ❌ Erreur WebDriver recherche éléments: {e}")
                return []
            
            except Exception as e:
                logging.error(f"   ❌ Erreur inattendue recherche éléments: {e}")
                return []
        
        return []

    def _find_all_reports(self):
        """Trouve tous les rapports financiers avec gestion d'erreurs robuste"""
        all_reports = defaultdict(list)
        
        if not self.driver:
            logging.error("❌ Driver Selenium non disponible")
            return all_reports
        
        try:
            url = "https://www.brvm.org/fr/capitalisation-marche"
            
            if not self._safe_get_page(url):
                logging.error("❌ Impossible d'accéder à la page principale")
                return all_reports
            
            # Chercher les liens vers les pages de sociétés
            company_links = set()
            try:
                elements = self._safe_find_elements(By.TAG_NAME, 'a')
                for elem in elements:
                    try:
                        href = elem.get_attribute('href')
                        if href and '/societe/' in href:
                            company_links.add(href)
                    except StaleElementReferenceException:
                        continue
                    except Exception as e:
                        logging.debug(f"   ⚠️  Erreur lecture href: {e}")
                        continue
            
            except Exception as e:
                logging.error(f"   ❌ Erreur collecte liens sociétés: {e}")
            
            company_links = list(company_links)
            logging.info(f"   📊 {len(company_links)} page(s) de société trouvée(s)")
            
            # Parcourir chaque page de société
            for idx, link in enumerate(company_links, 1):
                try:
                    logging.info(f"   📄 Analyse page {idx}/{len(company_links)}")
                    
                    if not self._safe_get_page(link):
                        logging.warning(f"   ⚠️  Page {link} inaccessible, passage à la suivante")
                        continue
                    
                    # Chercher les rapports financiers
                    report_elements = self._safe_find_elements(By.TAG_NAME, 'a')
                    
                    for elem in report_elements:
                        try:
                            href = elem.get_attribute('href')
                            text = elem.text.strip()
                            
                            if not href or not href.endswith('.pdf'):
                                continue
                            
                            if any(keyword in text.lower() for keyword in 
                                   ['rapport', 'financier', 'annuel', 'semestriel', 'trimestriel', 'etats financiers']):
                                
                                # Extraire la date
                                date_match = re.search(r'(20\d{2})', text)
                                report_date = datetime(int(date_match.group(1)), 12, 31).date() if date_match else datetime.now().date()
                                
                                # Identifier la société
                                for symbol, info in self.societes_mapping.items():
                                    nom_principal = self._normalize_text(info['nom_rapport'])
                                    alternatives = [self._normalize_text(alt) for alt in info.get('alternatives', [])]
                                    text_normalized = self._normalize_text(text)
                                    
                                    if nom_principal in text_normalized or any(alt in text_normalized for alt in alternatives):
                                        all_reports[symbol].append({
                                            'url': href,
                                            'titre': text,
                                            'date': report_date
                                        })
                                        break
                        
                        except StaleElementReferenceException:
                            continue
                        except AttributeError as e:
                            logging.debug(f"   ⚠️  Attribut manquant: {e}")
                            continue
                        except Exception as e:
                            logging.debug(f"   ⚠️  Erreur traitement élément: {e}")
                            continue
                
                except TimeoutException:
                    logging.warning(f"   ⏱️  Timeout sur page {link}")
                    continue
                except WebDriverException as e:
                    logging.error(f"   ❌ Erreur WebDriver page {link}: {e}")
                    continue
                except Exception as e:
                    logging.error(f"   ❌ Erreur inattendue page {link}: {e}")
                    continue
            
            total_reports = sum(len(r) for r in all_reports.values())
            logging.info(f"   ✅ Collecte terminée: {total_reports} rapport(s) trouvé(s)")
            return all_reports
        
        except Exception as e:
            logging.error(f"❌ Erreur critique recherche rapports: {e}")
            return all_reports

    def _get_next_api_key(self):
        """Obtient la prochaine clé API disponible"""
        try:
            key_info = self.api_manager.get_next_key()
            if key_info:
                self.current_api_key = key_info
                return key_info['key']
            return None
        except Exception as e:
            logging.error(f"❌ Erreur récupération clé API: {e}")
            return None

    def _analyze_pdf_with_api(self, company_id, symbol, report):
        """Analyse un PDF avec l'API Gemini - Gestion complète des erreurs"""
        pdf_url = report['url']
        
        # Vérification mémoire
        if pdf_url in self.analysis_memory:
            logging.info(f"    ⏭️  Déjà analysé")
            return None
        
        # Double vérification DB
        conn = self.connect_to_db()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM fundamental_analysis WHERE report_url = %s;", (pdf_url,))
                    if cur.fetchone():
                        logging.info(f"    ⏭️  Déjà en base")
                        self.analysis_memory.add(pdf_url)
                        return None
            except Exception as e:
                logging.warning(f"    ⚠️  Erreur vérification DB: {e}")
            finally:
                conn.close()
        
        logging.info(f"    🆕 NOUVEAU: {os.path.basename(pdf_url)}")
        
        # Télécharger le PDF une seule fois
        try:
            pdf_response = self.session.get(pdf_url, timeout=45, verify=False)
            pdf_response.raise_for_status()
            pdf_data = base64.b64encode(pdf_response.content).decode('utf-8')
        except requests.Timeout:
            logging.error(f"    ❌ Timeout téléchargement PDF")
            return False
        except requests.RequestException as e:
            logging.error(f"    ❌ Erreur téléchargement PDF: {e}")
            return False
        except Exception as e:
            logging.error(f"    ❌ Erreur encodage PDF: {e}")
            return False
        
        prompt = """Tu es un analyste financier expert. Analyse ce rapport financier et fournis une synthèse concise en français.

Concentre-toi sur :
- **Chiffre d'Affaires** : Variation en % et valeur
- **Résultat Net** : Évolution et facteurs
- **Dividendes** : Proposé, payé ou perspectives
- **Performance Opérationnelle** : Rentabilité
- **Perspectives** : Points clés pour investisseurs

Si une info manque, mentionne-le clairement."""
        
        # Boucle de retry avec toutes les clés disponibles
        max_attempts = len(self.api_manager.get_available_keys())
        attempts = 0
        
        while attempts < max_attempts:
            api_key = self._get_next_api_key()
            
            if not api_key:
                logging.error("    ❌ Aucune clé API disponible")
                return False
            
            key_num = self.current_api_key['number']
            logging.info(f"    🤖 Tentative avec clé #{key_num}")
            
            # Gestion rate limit
            self.api_manager.handle_rate_limit()
            
            api_url = f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models/{GEMINI_MODEL}:generateContent"
            
            headers = {
                "Content-Type": "application/json",
                "x-goog-api-key": api_key
            }
            
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
            
            try:
                response = requests.post(api_url, headers=headers, json=request_body, timeout=120)
                
                if response.status_code == 200:
                    response_json = response.json()
                    analysis_text = response_json['candidates'][0]['content']['parts'][0]['text']
                    
                    if "erreur" not in analysis_text.lower():
                        if self._save_to_db(company_id, report, analysis_text):
                            self.newly_analyzed_reports.append(f"Rapport {symbol}:\n{analysis_text}\n")
                            return True
                
                elif response.status_code == 429:
                    logging.warning(f"    ⚠️  Quota épuisé pour clé #{key_num}")
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                elif response.status_code == 404:
                    logging.error(f"    ❌ 404 avec clé #{key_num}")
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                elif response.status_code == 403:
                    logging.error(f"    ❌ 403 avec clé #{key_num}")
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                else:
                    logging.error(f"    ❌ Erreur {response.status_code}")
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                    
            except requests.Timeout:
                logging.error(f"    ❌ Timeout API clé #{key_num}")
                self.api_manager.move_to_next_key()
                attempts += 1
            except requests.RequestException as e:
                logging.error(f"    ❌ Erreur requête clé #{key_num}: {e}")
                self.api_manager.move_to_next_key()
                attempts += 1
            except KeyError as e:
                logging.error(f"    ❌ Réponse API malformée clé #{key_num}: {e}")
                self.api_manager.move_to_next_key()
                attempts += 1
            except Exception as e:
                logging.error(f"    ❌ Exception clé #{key_num}: {e}")
                self.api_manager.move_to_next_key()
                attempts += 1
        
        return False

    def run_and_get_results(self):
        """Fonction principale avec gestion d'erreurs complète"""
        logging.info("="*80)
        logging.info("📄 ÉTAPE 4: ANALYSE FONDAMENTALE (V10.0 FINALE OPTIMISÉE)")
        logging.info("="*80)
        
        conn = None
        try:
            # Afficher les stats initiales
            stats = self.api_manager.get_statistics()
            logging.info(f"📊 Clés disponibles: {stats['available']}/{stats['total']}")
            
            self._load_analysis_memory_from_db()
            logging.info(f"📊 Mémoire: {len(self.analysis_memory)} rapport(s) déjà analysé(s)")
            
            if not self.setup_selenium():
                logging.error("❌ Impossible de continuer sans Selenium")
                return {}, []
            
            conn = self.connect_to_db()
            if not conn: 
                logging.error("❌ Impossible de continuer sans connexion DB")
                return {}, []
            
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT symbol, id, name FROM companies")
                    companies_from_db = cur.fetchall()
            except Exception as e:
                logging.error(f"❌ Erreur récupération sociétés: {e}")
                return {}, []
            finally:
                conn.close()
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}
            
            logging.info("\n🔍 Phase 1: Collecte rapports...")
            all_reports = self._find_all_reports()
            
            total_reports = sum(len(r) for r in all_reports.values())
            logging.info(f"\n📊 {total_reports} rapport(s) trouvé(s)")
            
            logging.info(f"\n🤖 Phase 2: Analyse IA...")
            
            total_analyzed = 0
            total_skipped = 0
            total_errors = 0
            
            for symbol, (company_id, company_name) in self.company_ids.items():
                logging.info(f"\n📊 {symbol} - {company_name}")
                company_reports = all_reports.get(symbol, [])
                
                if not company_reports:
                    logging.info(f"   ⏭️  Aucun rapport")
                    continue
                
                try:
                    date_2024 = datetime(2024, 1, 1).date()
                    recent = [r for r in company_reports if r['date'] >= date_2024]
                    recent.sort(key=lambda x: x['date'], reverse=True)
                    
                    logging.info(f"   📂 {len(recent)} rapport(s) récent(s)")
                    
                    already = [r for r in recent if r['url'] in self.analysis_memory]
                    new = [r for r in recent if r['url'] not in self.analysis_memory]
                    
                    logging.info(f"   ✅ Déjà: {len(already)} | 🆕 Nouveaux: {len(new)}")
                    
                    for report in new:
                        try:
                            result = self._analyze_pdf_with_api(company_id, symbol, report)
                            if result is True:
                                total_analyzed += 1
                            elif result is None:
                                total_skipped += 1
                            else:
                                total_errors += 1
                        except Exception as e:
                            logging.error(f"   ❌ Erreur analyse rapport {report.get('titre', 'inconnu')}: {e}")
                            total_errors += 1
                    
                    total_skipped += len(already)
                
                except Exception as e:
                    logging.error(f"   ❌ Erreur traitement société {symbol}: {e}")
                    continue
            
            # Stats finales
            final_stats = self.api_manager.get_statistics()
            
            logging.info("\n✅ Traitement terminé")
            logging.info(f"📊 Nouvelles analyses: {total_analyzed}")
            logging.info(f"📊 Rapports ignorés: {total_skipped}")
            logging.info(f"📊 Erreurs: {total_errors}")
            logging.info(f"📊 Clés utilisées: {final_stats['used_by_module']}")
            logging.info(f"📊 Clés épuisées: {final_stats['exhausted']}")
            logging.info(f"📊 Clés disponibles: {final_stats['available']}")
            
            # Récupération résultats finaux
            conn = self.connect_to_db()
            if not conn: 
                return {}, []
            
            try:
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
            
            except Exception as e:
                logging.error(f"❌ Erreur récupération résultats: {e}")
                return {}, []
            finally:
                conn.close()
            
            logging.info(f"📊 Résultats: {len(final_results)} société(s)")
            return (dict(final_results), self.newly_analyzed_reports)
        
        except KeyboardInterrupt:
            logging.warning("\n⚠️  Interruption utilisateur détectée")
            return {}, []
        
        except Exception as e:
            logging.critical(f"❌ Erreur critique: {e}", exc_info=True)
            return {}, []
        
        finally:
            # Nettoyage garanti
            try:
                if self.driver: 
                    logging.info("🧹 Fermeture du navigateur...")
                    self.driver.quit()
            except Exception as e:
                logging.warning(f"⚠️  Erreur fermeture driver: {e}")
            
            try:
                if conn and not conn.closed: 
                    conn.close()
            except Exception as e:
                logging.warning(f"⚠️  Erreur fermeture connexion: {e}")

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
