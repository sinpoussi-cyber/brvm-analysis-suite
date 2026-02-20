# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER V28.0 - MULTI-AI (DeepSeek + Gemini + Mistral)
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
from collections import defaultdict
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
import psycopg2
import PyPDF2
import io

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION MULTI-AI (Rotation: DeepSeek → Gemini → Mistral)
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

MISTRAL_API_KEY = os.environ.get('MISTRAL_API_KEY')
MISTRAL_MODEL = "mistral-large-latest"
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"


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
        self.request_count = {'deepseek': 0, 'gemini': 0, 'mistral': 0}

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
            
            logging.info(f"   ✅ {len(self.analysis_memory)} analyse(s) chargée(s)")
                    
        except Exception as e:
            logging.error(f"❌ Erreur chargement mémoire: {e}")
            self.analysis_memory = set()
        finally:
            if conn: 
                conn.close()

    def _save_to_db(self, company_id, report, summary, ai_provider="unknown"):
        """Sauvegarde dans PostgreSQL avec indication du provider IA"""
        conn = self.connect_to_db()
        if not conn: 
            return False
        
        try:
            with conn.cursor() as cur:
                # Ajouter l'info du provider dans le summary
                enhanced_summary = f"[Analysé par {ai_provider.upper()}]\n\n{summary}"
                
                cur.execute("""
                    INSERT INTO fundamental_analysis (company_id, report_url, report_title, report_date, analysis_summary)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (report_url) DO UPDATE SET
                        analysis_summary = EXCLUDED.analysis_summary,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                """, (company_id, report['url'], report['titre'], report['date'], enhanced_summary))
                
                inserted_id = cur.fetchone()[0]
                conn.commit()
            
            self.analysis_memory.add(report['url'])
            logging.info(f"    ✅ Sauvegardé (ID: {inserted_id}, Provider: {ai_provider})")
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
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            logging.info("✅ Selenium prêt")
            return True
            
        except Exception as e:
            logging.error(f"❌ Erreur Selenium: {e}")
            return False

    def _find_all_reports(self):
        """Collecte tous les rapports disponibles sur le site BRVM"""
        all_reports = defaultdict(list)
        
        try:
            base_url = "https://www.brvm.org/fr/publications"
            self.driver.get(base_url)
            time.sleep(3)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            for symbol, info in self.societes_mapping.items():
                nom_rapport = info['nom_rapport']
                alternatives = info['alternatives']
                
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    text = link.get_text(strip=True).lower()
                    
                    if any(alt.lower() in text for alt in [nom_rapport.lower()] + alternatives):
                        if '.pdf' in href.lower():
                            full_url = href if href.startswith('http') else 'https://www.brvm.org' + href
                            
                            titre = link.get_text(strip=True)
                            date_match = re.search(r'(\d{4})', titre)
                            date_obj = datetime(int(date_match.group(1)), 12, 31).date() if date_match else datetime.now().date()
                            
                            all_reports[symbol].append({
                                'url': full_url,
                                'titre': titre,
                                'date': date_obj
                            })
            
            logging.info(f"✅ {sum(len(v) for v in all_reports.values())} rapport(s) collecté(s)")
            return all_reports
            
        except Exception as e:
            logging.error(f"❌ Erreur collecte rapports: {e}")
            return defaultdict(list)

    def _extract_text_from_pdf(self, pdf_url):
        """Extrait le texte d'un PDF"""
        try:
            response = requests.get(pdf_url, timeout=30, verify=False)
            pdf_file = io.BytesIO(response.content)
            
            text = ""
            with PyPDF2.PdfReader(pdf_file) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text += page_text + "\n"
            
            # Nettoyage
            text = re.sub(r'\s+', ' ', text).strip()
            text = unicodedata.normalize('NFKD', text)
            
            return text[:50000]  # Limiter à 50k caractères
            
        except Exception as e:
            logging.error(f"❌ Erreur extraction PDF: {e}")
            return None

    def _analyze_with_deepseek(self, text_content, symbol, report_title):
        """Analyse avec DeepSeek API"""
        if not DEEPSEEK_API_KEY:
            return None
        
        prompt = f"""Tu es un analyste financier expert. Analyse ce rapport financier de la société {symbol} ({report_title}).

RAPPORT:
{text_content}

CONSIGNES:
Fournis une analyse structurée en français couvrant:
1. Chiffre d'affaires et évolution
2. Résultat net et rentabilité
3. Politique de dividende
4. Perspectives et recommandations

Sois précis, factuel et concis (max 800 mots)."""

        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": DEEPSEEK_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 2000,
            "temperature": 0.3
        }
        
        try:
            response = requests.post(DEEPSEEK_API_URL, headers=headers, json=data, timeout=120)
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    analysis = result['choices'][0]['message']['content']
                    self.request_count['deepseek'] += 1
                    return analysis
            
            logging.warning(f"⚠️ DeepSeek erreur {response.status_code}")
            return None
            
        except Exception as e:
            logging.error(f"❌ DeepSeek exception: {e}")
            return None

    def _analyze_with_gemini(self, text_content, symbol, report_title):
        """Analyse avec Gemini API"""
        if not GEMINI_API_KEY:
            return None
        
        prompt = f"""Tu es un analyste financier expert. Analyse ce rapport financier de la société {symbol} ({report_title}).

RAPPORT:
{text_content}

CONSIGNES:
Fournis une analyse structurée en français couvrant:
1. Chiffre d'affaires et évolution
2. Résultat net et rentabilité
3. Politique de dividende
4. Perspectives et recommandations

Sois précis, factuel et concis (max 800 mots)."""

        url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
        
        data = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 2000
            }
        }
        
        try:
            response = requests.post(url, json=data, timeout=120)
            
            if response.status_code == 200:
                result = response.json()
                if 'candidates' in result and len(result['candidates']) > 0:
                    analysis = result['candidates'][0]['content']['parts'][0]['text']
                    self.request_count['gemini'] += 1
                    return analysis
            
            logging.warning(f"⚠️ Gemini erreur {response.status_code}")
            return None
            
        except Exception as e:
            logging.error(f"❌ Gemini exception: {e}")
            return None

    def _analyze_with_mistral(self, text_content, symbol, report_title):
        """Analyse avec Mistral API"""
        if not MISTRAL_API_KEY:
            return None
        
        prompt = f"""Tu es un analyste financier expert. Analyse ce rapport financier de la société {symbol} ({report_title}).

RAPPORT:
{text_content}

CONSIGNES:
Fournis une analyse structurée en français couvrant:
1. Chiffre d'affaires et évolution
2. Résultat net et rentabilité
3. Politique de dividende
4. Perspectives et recommandations

Sois précis, factuel et concis (max 800 mots)."""

        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": MISTRAL_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 2500,
            "temperature": 0.3
        }
        
        try:
            response = requests.post(MISTRAL_API_URL, headers=headers, json=data, timeout=120)
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    analysis = result['choices'][0]['message']['content']
                    self.request_count['mistral'] += 1
                    return analysis
            
            logging.warning(f"⚠️ Mistral erreur {response.status_code}")
            return None
            
        except Exception as e:
            logging.error(f"❌ Mistral exception: {e}")
            return None

    def _analyze_pdf_with_multi_ai(self, company_id, symbol, report):
        """Analyse un rapport avec rotation automatique des API (DeepSeek → Gemini → Mistral)"""
        
        # Vérifier si déjà analysé
        if report['url'] in self.analysis_memory:
            logging.info(f"    ⏭️  Déjà analysé: {report['titre']}")
            return None
        
        logging.info(f"    📄 {report['titre'][:80]}...")
        
        # Extraire le texte du PDF
        text_content = self._extract_text_from_pdf(report['url'])
        
        if not text_content or len(text_content) < 100:
            logging.warning(f"    ⚠️  PDF vide ou illisible")
            return False
        
        logging.info(f"    ✓ Texte extrait: {len(text_content)} caractères")
        
        # ROTATION DES API: DeepSeek → Gemini → Mistral
        analysis = None
        provider_used = None
        
        # Tentative 1: DeepSeek (priorité 1)
        logging.info("    🤖 Tentative DeepSeek...")
        analysis = self._analyze_with_deepseek(text_content, symbol, report['titre'])
        if analysis:
            provider_used = "deepseek"
            logging.info("    ✅ DeepSeek: Succès!")
        else:
            # Tentative 2: Gemini (priorité 2)
            logging.info("    🤖 Tentative Gemini...")
            analysis = self._analyze_with_gemini(text_content, symbol, report['titre'])
            if analysis:
                provider_used = "gemini"
                logging.info("    ✅ Gemini: Succès!")
            else:
                # Tentative 3: Mistral (priorité 3)
                logging.info("    🤖 Tentative Mistral...")
                analysis = self._analyze_with_mistral(text_content, symbol, report['titre'])
                if analysis:
                    provider_used = "mistral"
                    logging.info("    ✅ Mistral: Succès!")
        
        # Si aucune API n'a fonctionné
        if not analysis:
            logging.error(f"    ❌ Échec des 3 API pour {symbol}")
            fallback_text = f"Analyse automatique indisponible. Rapport: {report['titre']}"
            self._save_to_db(company_id, report, fallback_text, "fallback")
            return False
        
        # Sauvegarde
        if self._save_to_db(company_id, report, analysis, provider_used):
            self.newly_analyzed_reports.append(f"Rapport {symbol} (via {provider_used}):\n{analysis}\n")
            return True
        
        return False

    def run_and_get_results(self):
        """Fonction principale"""
        logging.info("="*80)
        logging.info("📄 ÉTAPE 4: ANALYSE FONDAMENTALE (V28.0 - Multi-AI)")
        logging.info("🤖 Providers: DeepSeek → Gemini → Mistral (rotation automatique)")
        logging.info("="*80)
        
        conn = None
        try:
            # Vérifier qu'au moins une clé API est disponible
            if not any([DEEPSEEK_API_KEY, GEMINI_API_KEY, MISTRAL_API_KEY]):
                logging.error("❌ Aucune clé API configurée!")
                return {}, []
            
            available_apis = []
            if DEEPSEEK_API_KEY:
                available_apis.append("DeepSeek")
            if GEMINI_API_KEY:
                available_apis.append("Gemini")
            if MISTRAL_API_KEY:
                available_apis.append("Mistral")
            
            logging.info(f"✅ API disponibles: {', '.join(available_apis)}")
            
            self._load_analysis_memory_from_db()
            
            if not self.setup_selenium():
                logging.error("❌ Impossible d'initialiser Selenium")
                return {}, []
            
            conn = self.connect_to_db()
            if not conn: 
                return {}, []
            
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, id, name FROM companies")
                companies_from_db = cur.fetchall()
            conn.close()
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}
            
            logging.info(f"\n🔍 Phase 1: Collecte rapports...")
            all_reports = self._find_all_reports()
            
            logging.info(f"\n🤖 Phase 2: Analyse Multi-AI (DeepSeek → Gemini → Mistral)...")
            
            total_analyzed = 0
            total_skipped = 0
            
            for symbol, (company_id, company_name) in self.company_ids.items():
                logging.info(f"\n📊 {symbol} - {company_name}")
                company_reports = all_reports.get(symbol, [])
                
                if not company_reports:
                    logging.info(f"   ⏭️  Aucun rapport")
                    continue
                
                date_2024 = datetime(2024, 1, 1).date()
                recent = [r for r in company_reports if r['date'] >= date_2024]
                recent.sort(key=lambda x: x['date'], reverse=True)
                
                logging.info(f"   📂 {len(recent)} rapport(s) récent(s)")
                
                already = [r for r in recent if r['url'] in self.analysis_memory]
                new = [r for r in recent if r['url'] not in self.analysis_memory]
                
                logging.info(f"   ✅ Déjà: {len(already)} | 🆕 Nouveaux: {len(new)}")
                
                for report in new:
                    result = self._analyze_pdf_with_multi_ai(company_id, symbol, report)
                    if result is True:
                        total_analyzed += 1
                    elif result is None:
                        total_skipped += 1
                
                total_skipped += len(already)
            
            logging.info("\n✅ Traitement terminé")
            logging.info(f"📊 Nouvelles analyses: {total_analyzed}")
            logging.info(f"📊 Rapports ignorés: {total_skipped}")
            logging.info(f"📊 Statistiques requêtes:")
            logging.info(f"   - DeepSeek: {self.request_count['deepseek']}")
            logging.info(f"   - Gemini: {self.request_count['gemini']}")
            logging.info(f"   - Mistral: {self.request_count['mistral']}")
            
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
