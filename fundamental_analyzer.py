# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER (V1.5 - GESTION DES CLÉS ÉTENDUE)
# ==============================================================================

# --- Imports ---
import gspread
import requests
from bs4 import BeautifulSoup
import time
import re
from docx import Document
from docx.shared import Pt
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
from google.oauth2 import service_account
import google.generativeai as genai
from google.api_core import exceptions as api_exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ANALYSIS_MEMORY_SHEET = 'ANALYSIS_MEMORY'

class BRVMAnalyzer:
    def __init__(self, spreadsheet_id):
        self.spreadsheet_id = spreadsheet_id
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
        self.gc = None
        self.driver = None
        self.gemini_model = None
        self.original_societes_mapping = self.societes_mapping.copy()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        
        self.spreadsheet = None
        self.memory_worksheet = None
        self.analysis_memory = {}
        
        self.api_keys = []
        self.current_key_index = 0

    def setup_selenium(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("--window-size=1920,1080")
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logging.info("✅ Pilote Selenium (Chrome) démarré.")
        except Exception as e:
            logging.error(f"❌ Impossible de démarrer le pilote Selenium: {e}")
            self.driver = None
            
    def authenticate_google_services(self):
        logging.info("Authentification Google...")
        try:
            creds_json_str = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
            if not creds_json_str:
                logging.error("❌ Secret GSPREAD_SERVICE_ACCOUNT introuvable.")
                return False
            creds_dict = json.loads(creds_json_str)
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.gc = gspread.authorize(creds)
            logging.info("✅ Authentification Google réussie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur d'authentification : {e}")
            return False

    def _load_analysis_memory(self):
        logging.info("Chargement de la mémoire d'analyse...")
        try:
            self.memory_worksheet = self.spreadsheet.worksheet(ANALYSIS_MEMORY_SHEET)
            logging.info(f"Feuille de mémoire '{ANALYSIS_MEMORY_SHEET}' trouvée.")
        except gspread.exceptions.WorksheetNotFound:
            logging.warning(f"Feuille de mémoire '{ANALYSIS_MEMORY_SHEET}' non trouvée. Création en cours...")
            self.memory_worksheet = self.spreadsheet.add_worksheet(title=ANALYSIS_MEMORY_SHEET, rows=2000, cols=4)
            headers = ['URL', 'Symbol', 'Analysis_Summary', 'Analysis_Date']
            self.memory_worksheet.update('A1:D1', [headers])
            logging.info(f"Feuille '{ANALYSIS_MEMORY_SHEET}' créée avec les en-têtes.")

        try:
            records = self.memory_worksheet.get_all_records()
            self.analysis_memory = {row['URL']: row['Analysis_Summary'] for row in records if row.get('URL')}
            logging.info(f"{len(self.analysis_memory)} analyses pré-existantes chargées en mémoire.")
        except Exception as e:
            logging.error(f"Impossible de charger les enregistrements de la mémoire : {e}")

    def _save_to_memory(self, symbol, report_url, summary):
        if not self.memory_worksheet:
            logging.error("Impossible de sauvegarder en mémoire, la feuille n'est pas initialisée.")
            return
        
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row_to_add = [report_url, symbol, summary, timestamp]
            self.memory_worksheet.append_row(row_to_add, value_input_option='USER_ENTERED')
            
            self.analysis_memory[report_url] = summary
            logging.info(f"    -> Analyse pour {symbol} sauvegardée dans la mémoire persistante.")
        except Exception as e:
            logging.error(f"    -> ERREUR lors de la sauvegarde en mémoire : {e}")

    def _configure_gemini_with_rotation(self):
        """Charge toutes les clés API Gemini depuis les variables d'environnement."""
        # MODIFIÉ : Boucle étendue pour chercher plus de clés
        for i in range(1, 20): 
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                self.api_keys.append(key)

        if not self.api_keys:
            logging.error("❌ Aucune clé API nommée 'GOOGLE_API_KEY_n' n'a été trouvée dans les secrets GitHub.")
            return False
            
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini ont été chargées.")
        
        try:
            genai.configure(api_key=self.api_keys[self.current_key_index])
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash-latest')
            logging.info(f"API Gemini configurée avec la clé #{self.current_key_index + 1}.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur de configuration avec la clé #{self.current_key_index + 1}: {e}")
            return self._rotate_api_key()

    def _rotate_api_key(self):
        """Passe à la clé API suivante dans la liste."""
        self.current_key_index += 1
        if self.current_key_index >= len(self.api_keys):
            logging.error("❌ Toutes les clés API Gemini ont été épuisées ou sont invalides.")
            return False
        
        logging.warning(f"Passage à la clé API Gemini #{self.current_key_index + 1}...")
        try:
            genai.configure(api_key=self.api_keys[self.current_key_index])
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash-latest')
            logging.info(f"API Gemini reconfigurée avec succès avec la clé #{self.current_key_index + 1}.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur de configuration avec la clé #{self.current_key_index + 1}: {e}")
            return self._rotate_api_key()

    def _analyze_pdf_with_gemini(self, pdf_url, symbol):
        if pdf_url in self.analysis_memory:
            logging.info(f"    -> Analyse pour {pdf_url} trouvée en mémoire. Utilisation de la version en cache.")
            return self.analysis_memory[pdf_url]

        if not self.gemini_model:
            return "Analyse IA non disponible (API non configurée)."
        
        max_retries = len(self.api_keys)
        for attempt in range(max_retries):
            try:
                logging.info(f"    -> Nouvelle analyse IA (clé #{self.current_key_index + 1}) : Téléchargement du PDF...")
                response = self.session.get(pdf_url, timeout=45, verify=False)
                response.raise_for_status()
                pdf_content = response.content
                if len(pdf_content) < 1024:
                    return "Fichier PDF invalide ou vide."
                
                uploaded_file = genai.upload_file(files=[{'name': 'report.pdf', 'display_name': 'Rapport Financier BRVM', 'mime_type': 'application/pdf', 'content': pdf_content}])
                
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
                
                logging.info("    -> Fichier envoyé. Génération de l'analyse...")
                response = self.gemini_model.generate_content([prompt, uploaded_file])
                
                analysis_text = ""
                if response.parts:
                    analysis_text = response.text
                elif response.prompt_feedback:
                    block_reason = response.prompt_feedback.block_reason.name
                    analysis_text = f"Analyse bloquée par l'IA. Raison : {block_reason}."
                else:
                    analysis_text = "Erreur inconnue : L'API Gemini n'a retourné ni contenu ni feedback."
                
                genai.delete_file(uploaded_file.name)
                
                if analysis_text and "erreur" not in analysis_text.lower() and "bloquée" not in analysis_text.lower():
                    self._save_to_memory(symbol, pdf_url, analysis_text)
                
                return analysis_text
            
            except api_exceptions.ResourceExhausted as e:
                logging.warning(f"Quota atteint pour la clé API #{self.current_key_index + 1}. ({e})")
                if not self._rotate_api_key():
                    return "Erreur d'analyse : Toutes les clés API ont atteint leur quota."
            
            except Exception as e:
                logging.error(f"    -> Erreur technique inattendue lors de l'analyse IA : {e}")
                return f"Erreur technique lors de l'analyse par l'IA : {str(e)}"
        
        return "Erreur d'analyse : Échec après avoir essayé toutes les clés API."

    def run_and_get_results(self):
        logging.info("="*60)
        logging.info("ÉTAPE 3 : DÉMARRAGE DE L'ANALYSE FONDAMENTALE (IA)")
        logging.info("="*60)
        
        analysis_results = {}
        try:
            if not self._configure_gemini_with_rotation(): return {}
            if not self.authenticate_google_services(): return {}
            
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            self._load_analysis_memory()

            self.setup_selenium()
            if not self.driver: return {}
            if not self.verify_and_filter_companies(): return {}
            
            analysis_results = self.process_all_companies()
            
            if not analysis_results:
                logging.warning("❌ Aucun résultat d'analyse à retourner.")

        except Exception as e:
            logging.critical(f"❌ Une erreur critique a interrompu l'analyse fondamentale: {e}", exc_info=True)
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Navigateur Selenium fermé.")
            logging.info("Processus d'analyse fondamentale terminé.")
        
        return analysis_results

    # ... Le reste des fonctions (_verify_and_filter_companies, _normalize_text, etc.) est inchangé ...
