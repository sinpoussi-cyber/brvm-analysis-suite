# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER (V1.7 - CORRECTION UPLOAD API)
# ==============================================================================

# ... (tous les imports et le début de la classe restent inchangés) ...
# ... (copiez-collez tout le début du fichier jusqu'à la fonction _analyze_pdf_with_gemini)
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
        # ... (le mapping des sociétés est ici) ...
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
        self.newly_analyzed_reports = []

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
                
                # CORRECTION : La méthode d'upload a changé. On utilise `path` pour un fichier temporaire.
                temp_pdf_path = "temp_report.pdf"
                with open(temp_pdf_path, 'wb') as f:
                    f.write(pdf_content)

                uploaded_file = genai.upload_file(path=temp_pdf_path, display_name="Rapport Financier BRVM")
                
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
                os.remove(temp_pdf_path) # Nettoyage du fichier local
                
                if analysis_text and "erreur" not in analysis_text.lower() and "bloquée" not in analysis_text.lower():
                    self._save_to_memory(symbol, pdf_url, analysis_text)
                    self.newly_analyzed_reports.append(f"Rapport pour {symbol} ({os.path.basename(pdf_url)}):\n{analysis_text}\n")
                
                return analysis_text
            
            except api_exceptions.ResourceExhausted as e:
                logging.warning(f"Quota atteint pour la clé API #{self.current_key_index + 1}. ({e})")
                if not self._rotate_api_key():
                    return "Erreur d'analyse : Toutes les clés API ont atteint leur quota."
            
            except Exception as e:
                logging.error(f"    -> Erreur technique inattendue lors de l'analyse IA : {e}")
                # Nettoyage en cas d'erreur
                if 'uploaded_file' in locals() and uploaded_file:
                    try: genai.delete_file(uploaded_file.name)
                    except: pass
                if os.path.exists("temp_report.pdf"):
                    os.remove("temp_report.pdf")
                return f"Erreur technique lors de l'analyse par l'IA : {str(e)}"
        
        return "Erreur d'analyse : Échec après avoir essayé toutes les clés API."

    # ... (toutes les autres fonctions du fichier sont correctes et restent inchangées)
    # Copiez-collez l'intégralité du reste du fichier tel qu'il était.
