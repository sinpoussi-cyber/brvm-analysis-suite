# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR (V3.0 - POSTGRESQL FINAL)
# ==============================================================================

import psycopg2
import pandas as pd
import os
import json
import time
import logging
from docx import Document
from io import BytesIO
import google.generativeai as genai
from google.api_core import exceptions as api_exceptions
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')


class ComprehensiveReportGenerator:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.drive_service = None
        self.gemini_model = None
        self.api_keys = []
        self.current_key_index = 0

    def _authenticate_drive(self):
        try:
            creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
            scopes = ['https://www.googleapis.com/auth/drive']
            creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.drive_service = build('drive', 'v3', credentials=creds)
            logging.info("✅ Authentification Google Drive réussie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur d'authentification Drive : {e}")
            return False

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

    def _call_gemini_with_retry(self, prompt):
        # ... (identique à la version précédente) ...
        return "Erreur d'analyse : Échec après avoir essayé toutes les clés API."

    def _upload_to_drive(self, filepath):
        # ... (identique à la version précédente) ...

    def _get_company_data(self):
        # ... (logique pour récupérer toutes les données de la DB) ...
        return {}

    def generate_all_reports(self):
        if not self._authenticate_drive() or not self._configure_gemini_with_rotation():
            logging.error("Arrêt de la génération des rapports en raison d'un problème d'initialisation.")
            return

        company_data = self._get_company_data()
        
        # ... (logique de génération de rapports) ...

        # Exemple :
        # main_report_path = self._create_main_report(company_data)
        # self._upload_to_drive(main_report_path)
        # etc.

if __name__ == "__main__":
    db_connection = None
    try:
        db_connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        report_generator = ComprehensiveReportGenerator(db_connection)
        report_generator.generate_all_reports()
    except Exception as e:
        logging.error(f"❌ Erreur fatale dans le générateur de rapports : {e}", exc_info=True)
    finally:
        if db_connection:
            db_connection.close()
