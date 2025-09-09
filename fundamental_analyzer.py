# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER (V1.3 - GESTION DES CLÉS ROBUSTE)
# Description: Analyse les rapports financiers avec l'IA Gemini et gère
#              plusieurs clés API pour contourner les limites de quota.
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
            # ... (mapping des sociétés inchangé)
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

    # ... (les autres fonctions restent identiques)

    # MODIFIÉ : Simplification de la logique de chargement des clés
    def _configure_gemini_with_rotation(self):
        """Charge toutes les clés API Gemini depuis les variables d'environnement."""
        main_key = os.environ.get('GOOGLE_API_KEY_1')
        if main_key:
            self.api_keys.append(main_key)
        
        for i in range(2, 10): # Commence à 2 car la clé principale est déjà ajoutée
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                self.api_keys.append(key)

        if not self.api_keys:
            logging.error("❌ Aucune clé GOOGLE_API_KEY ou GOOGLE_API_KEY_n trouvée.")
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

    # ... (le reste du fichier fundamental_analyzer.py est inchangé)
