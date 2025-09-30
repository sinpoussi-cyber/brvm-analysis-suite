# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR (V4.0 - POSTGRESQL & APPEL API DIRECT)
# ==============================================================================

import psycopg2
import pandas as pd
import os
import json
import time
import logging
from docx import Document
from io import BytesIO
import requests
import base64
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

# Limite de requêtes Gemini par minute
REQUESTS_PER_MINUTE_LIMIT = 10

class ComprehensiveReportGenerator:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.drive_service = None
        self.api_keys = []
        self.current_key_index = 0
        self.request_timestamps = []

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

    def _configure_api_keys(self):
        for i in range(1, 20):
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key: self.api_keys.append(key)
        if not self.api_keys:
            logging.error("❌ Aucune clé API trouvée.")
            return False
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées.")
        return True

    def _call_gemini_with_retry(self, prompt):
        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        if len(self.request_timestamps) >= REQUESTS_PER_MINUTE_LIMIT:
            sleep_time = 60 - (now - self.request_timestamps[0])
            logging.warning(f"Limite de requêtes/minute atteinte. Pause de {sleep_time:.1f} secondes...")
            time.sleep(sleep_time)
            self.request_timestamps = [ts for ts in self.request_timestamps if time.time() - ts < 60]

        while self.current_key_index < len(self.api_keys):
            api_key = self.api_keys[self.current_key_index]
            api_url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={api_key}"
            
            try:
                self.request_timestamps.append(time.time())
                request_body = {"contents": [{"parts": [{"text": prompt}]}]}
                response = requests.post(api_url, json=request_body)

                if response.status_code == 429:
                    logging.warning(f"Quota atteint pour la clé API #{self.current_key_index + 1}.")
                    self.current_key_index += 1
                    continue
                
                response.raise_for_status()
                response_json = response.json()
                return response_json['candidates'][0]['content']['parts'][0]['text']

            except Exception as e:
                logging.error(f"Erreur avec la clé #{self.current_key_index + 1}: {e}")
                self.current_key_index += 1

        return "Erreur d'analyse : Toutes les clés API ont échoué."

    def _upload_to_drive(self, filepath):
        if not self.drive_service:
            logging.error("Service Drive non authentifié.")
            return
        try:
            file_metadata = {'name': os.path.basename(filepath), 'parents': [DRIVE_FOLDER_ID]}
            media = MediaFileUpload(filepath, mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            self.drive_service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
            logging.info(f"✅ Fichier '{os.path.basename(filepath)}' sauvegardé sur Google Drive.")
        except Exception as e:
            logging.error(f"❌ Erreur lors de la sauvegarde sur Drive : {e}")

    def _get_company_data_from_db(self):
        logging.info("Récupération de toutes les données depuis PostgreSQL...")
        query = """
        SELECT
            c.symbol,
            c.name as company_name,
            hd.trade_date,
            hd.price,
            ta.mm5, ta.mm10, ta.mm20, ta.mm50, ta.mm_decision,
            ta.bollinger_central, ta.bollinger_inferior, ta.bollinger_superior, ta.bollinger_decision,
            ta.macd_line, ta.signal_line, ta.histogram, ta.macd_decision,
            ta.rsi, ta.rsi_decision,
            ta.stochastic_k, ta.stochastic_d, ta.stochastic_decision
        FROM companies c
        JOIN historical_data hd ON c.id = hd.company_id
        LEFT JOIN technical_analysis ta ON hd.id = ta.historical_data_id
        ORDER BY c.symbol, hd.trade_date DESC;
        """
        df = pd.read_sql(query, self.db_conn)
        
        # Récupérer les analyses fondamentales
        query_fa = "SELECT c.symbol, fa.analysis_summary FROM fundamental_analysis fa JOIN companies c ON fa.company_id = c.id;"
        df_fa = pd.read_sql(query_fa, self.db_conn)
        
        fa_dict = df_fa.groupby('symbol')['analysis_summary'].apply(list).to_dict()

        company_reports = {}
        for symbol, group in df.groupby('symbol'):
            logging.info(f"--- Préparation des données pour : {symbol} ---")
            company_reports[symbol] = {
                'nom_societe': group['company_name'].iloc[0],
                'price_data': group[['trade_date', 'price']].head(50),
                'indicator_data': group.head(1), # Prendre la ligne la plus récente
                'fundamental_summaries': fa_dict.get(symbol, ["Aucune analyse fondamentale disponible."])
            }
        
        logging.info("✅ Toutes les données ont été récupérées et structurées.")
        return company_reports

    def _analyze_price_evolution(self, df_prices):
        # ... (identique à la version précédente)
        pass

    def _analyze_technical_indicators(self, df_indicators):
        # ... (identique à la version précédente)
        pass

    def _summarize_fundamental_analysis(self, summaries):
        # ... (identique à la version précédente)
        pass

    def _create_main_report(self, company_reports):
        # ... (identique à la version précédente)
        pass
    
    def generate_all_reports(self, fundamental_results, new_fundamental_analyses):
        logging.info("="*60)
        logging.info("ÉTAPE 4 : DÉMARRAGE DE LA GÉNÉRATION DES RAPPORTS (VERSION POSTGRESQL)")
        logging.info("="*60)

        if not self._authenticate_drive() or not self._configure_api_keys():
            return
            
        company_data = self._get_company_data_from_db()
        
        # Logique de génération des rapports
        main_doc_path = self._create_main_report(company_data)
        if main_doc_path:
            self._upload_to_drive(main_doc_path)
        
        # (Les rapports delta et événements seront ajoutés plus tard pour simplifier)

if __name__ == "__main__":
    db_connection = None
    try:
        db_connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        # Note: Pour le test, on passe des dictionnaires vides pour les résultats fondamentaux
        report_generator = ComprehensiveReportGenerator(db_connection)
        report_generator.generate_all_reports({}, [])
    except Exception as e:
        logging.error(f"❌ Erreur fatale dans le générateur de rapports : {e}", exc_info=True)
    finally:
        if db_connection:
            db_connection.close()
