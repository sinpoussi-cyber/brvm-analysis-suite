# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR (V4.1 - RAPPORTS COMPLETS)
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
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from collections import defaultdict
from datetime import datetime

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')

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
            logging.warning(f"Limite de requêtes/minute atteinte. Pause de {sleep_time + 1:.1f} secondes...")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []

        while self.current_key_index < len(self.api_keys):
            api_key = self.api_keys[self.current_key_index]
            api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
            try:
                self.request_timestamps.append(time.time())
                request_body = {"contents": [{"parts": [{"text": prompt}]}]}
                response = requests.post(api_url, json=request_body, timeout=60)
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
        if not self.drive_service: return
        try:
            file_metadata = {'name': os.path.basename(filepath), 'parents': [DRIVE_FOLDER_ID]}
            media = MediaFileUpload(filepath, mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            self.drive_service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
            logging.info(f"✅ Fichier '{os.path.basename(filepath)}' sauvegardé sur Google Drive.")
        except Exception as e:
            logging.error(f"❌ Erreur lors de la sauvegarde sur Drive : {e}")

    def _get_all_data_from_db(self):
        logging.info("Récupération de toutes les données d'analyse depuis PostgreSQL...")
        query = """
        WITH latest_historical_data AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
            FROM historical_data
        )
        SELECT
            c.symbol, c.name as company_name,
            lhd.trade_date, lhd.price,
            ta.mm5, ta.mm10, ta.mm20, ta.mm50, ta.mm_decision,
            ta.bollinger_central, ta.bollinger_inferior, ta.bollinger_superior, ta.bollinger_decision,
            ta.macd_line, ta.signal_line, ta.histogram, ta.macd_decision,
            ta.rsi, ta.rsi_decision,
            ta.stochastic_k, ta.stochastic_d, ta.stochastic_decision,
            (SELECT STRING_AGG(fa.analysis_summary, E'\\n---\\n' ORDER BY fa.report_date DESC) FROM fundamental_analysis fa WHERE fa.company_id = c.id) as fundamental_summaries
        FROM companies c
        LEFT JOIN latest_historical_data lhd ON c.id = lhd.company_id
        LEFT JOIN technical_analysis ta ON lhd.id = ta.historical_data_id
        WHERE lhd.rn <= 50 OR lhd.rn IS NULL;
        """
        df = pd.read_sql(query, self.db_conn)
        
        company_data = {}
        for symbol, group in df.groupby('symbol'):
            company_data[symbol] = {
                'nom_societe': group['company_name'].iloc[0],
                'price_data': group[['trade_date', 'price']].sort_values('trade_date').reset_index(drop=True),
                'indicator_data': group.sort_values('trade_date', ascending=False).iloc[0],
                'fundamental_summaries': group['fundamental_summaries'].iloc[0] or "Aucune analyse fondamentale disponible."
            }
        logging.info(f"✅ Données pour {len(company_data)} sociétés récupérées et structurées.")
        return company_data

    def _analyze_price_evolution(self, df_prices):
        if df_prices.empty or df_prices['price'].isnull().all():
            return "Données de prix insuffisantes pour une analyse."
        data_string = df_prices.to_csv(index=False)
        prompt = f"Analyse l'évolution du cours de cette action sur les 50 derniers jours. Tendance générale (haussière, baissière, stable)? Chiffres clés (début, fin, %, plus haut, plus bas)? Dynamique récente?\n\nDonnées:\n{data_string}"
        return self._call_gemini_with_retry(prompt)

    def _analyze_technical_indicators(self, series_indicators):
        data_string = series_indicators.to_string()
        prompt = f"Analyse ces indicateurs techniques pour le jour le plus récent. Pour chaque indicateur (MM, Bollinger, MACD, RSI, Stochastique), donne une analyse de 2-3 phrases et un signal clair (Achat, Vente, Neutre).\n\nIndicateurs:\n{data_string}"
        return self._call_gemini_with_retry(prompt)

    def _summarize_fundamental_analysis(self, summaries):
        prompt = f"Synthétise ces analyses de rapports financiers en 3 ou 4 points clés pour un investisseur, en te concentrant sur le chiffre d'affaires, le résultat net, les dividendes et les perspectives.\n\nAnalyses:\n{summaries}"
        return self._call_gemini_with_retry(prompt)

    def _create_main_report(self, company_analyses):
        logging.info("Création du rapport de synthèse principal...")
        doc = Document()
        doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        doc.add_paragraph(f"Généré le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        for symbol, analyses in sorted(company_analyses.items()):
            nom_societe = analyses.get('nom_societe', symbol)
            doc.add_heading(f'Analyse Détaillée : {nom_societe} ({symbol})', level=1)
            
            doc.add_heading('1. Évolution du Cours (50 derniers jours)', level=2)
            doc.add_paragraph(analyses.get('price_analysis', 'Analyse du prix non disponible.'))
            
            doc.add_heading('2. Analyse Technique des Indicateurs', level=2)
            doc.add_paragraph(analyses.get('technical_analysis', 'Analyse technique non disponible.'))
            
            doc.add_heading('3. Synthèse Fondamentale', level=2)
            doc.add_paragraph(analyses.get('fundamental_summary', 'Analyse fondamentale non disponible.'))
            
            doc.add_page_break()

        output_filename = f"Rapport_Synthese_Investissement_BRVM_{time.strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_filename)
        logging.info(f"🎉 Rapport de synthèse principal généré : {output_filename}")
        return output_filename

    def _generate_market_events_report(self, new_analyses):
        if not new_analyses:
            logging.info("Aucun nouvel événement fondamental à signaler.")
            return

        logging.info("Génération du rapport des événements marquants...")
        full_text = "\n---\n".join(new_analyses)
        prompt = f"""
        Tu es un journaliste financier. Rédige un court article de synthèse sur les événements marquants du jour sur la BRVM, basé sur les analyses de rapports suivantes.
        Structure ton article ainsi :
        1. Un titre accrocheur.
        2. Un paragraphe d'introduction.
        3. Des points clés pour les 2 ou 3 annonces les plus importantes (résultats, dividendes, perspectives...).
        4. Un court paragraphe de conclusion.

        Analyses du jour :
        {full_text}
        """
        market_summary = self._call_gemini_with_retry(prompt)
        
        doc = Document()
        doc.add_heading("Synthèse des Événements Marquants du Marché", level=0)
        doc.add_paragraph(f"Basé sur les rapports analysés le {datetime.now().strftime('%Y-%m-%d')}")
        doc.add_paragraph(market_summary)
        
        output_filename = f"Synthese_Marche_{time.strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_filename)
        logging.info(f"🎉 Rapport des événements marquants généré : {output_filename}")
        self._upload_to_drive(output_filename)

    def _generate_delta_report(self, new_report_path):
        if not self.drive_service: return

        logging.info("Recherche du dernier rapport sur Drive pour comparaison...")
        query = f"'{DRIVE_FOLDER_ID}' in parents and name contains 'Rapport_Synthese_Investissement_BRVM_' and mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document'"
        results = self.drive_service.files().list(q=query, pageSize=1, fields="files(id, name)", orderBy="createdTime desc", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        
        latest_file = results.get('files', [None])[0]
        if not latest_file:
            logging.info("  -> Aucun rapport précédent trouvé. Le rapport comparatif ne sera pas généré.")
            return

        logging.info(f"  -> Téléchargement du rapport précédent : {latest_file['name']}")
        request = self.drive_service.files().get_media(fileId=latest_file['id'], supportsAllDrives=True)
        old_file_bytes = BytesIO()
        old_file_bytes.write(request.execute())
        old_file_bytes.seek(0)

        old_doc = Document(old_file_bytes)
        new_doc = Document(new_report_path)
        old_text = "\n".join([p.text for p in old_doc.paragraphs])
        new_text = "\n".join([p.text for p in new_doc.paragraphs])

        prompt = f"""
        Compare les deux rapports suivants (ANCIEN et NOUVEAU) et rédige une synthèse des changements significatifs pour un investisseur.
        Concentre-toi sur les nouvelles informations fondamentales ou les changements de signaux techniques.
        Ignore les changements mineurs. Présente le résultat en points clés.

        --- ANCIEN RAPPORT ---
        {old_text[:15000]} 
        --- NOUVEAU RAPPORT ---
        {new_text[:15000]}
        """
        delta_summary = self._call_gemini_with_retry(prompt)
            
        delta_doc = Document()
        delta_doc.add_heading(f"Analyse des Changements entre Rapports", level=0)
        delta_doc.add_paragraph(f"Comparaison entre le rapport du jour et le rapport '{latest_file['name']}'.")
        delta_doc.add_heading("Synthèse des Changements Significatifs", level=1)
        delta_doc.add_paragraph(delta_summary)
        
        output_filename = f"Rapport_Comparatif_{time.strftime('%Y%m%d_%H%M')}.docx"
        delta_doc.save(output_filename)
        logging.info(f"🎉 Rapport comparatif généré : {output_filename}")
        self._upload_to_drive(output_filename)

    def generate_all_reports(self, new_fundamental_analyses):
        logging.info("="*60)
        logging.info("ÉTAPE 4 : DÉMARRAGE DE LA GÉNÉRATION DES RAPPORTS (VERSION POSTGRESQL)")
        logging.info("="*60)

        if not self._authenticate_drive() or not self._configure_api_keys():
            return
            
        all_data = self._get_all_data_from_db()
        company_analyses = {}

        for symbol, data in all_data.items():
            logging.info(f"--- Génération des synthèses IA pour : {symbol} ---")
            company_analyses[symbol] = {
                'nom_societe': data['nom_societe'],
                'price_analysis': self._analyze_price_evolution(data['price_data']),
                'technical_analysis': self._analyze_technical_indicators(data['indicator_data']),
                'fundamental_summary': self._summarize_fundamental_analysis(data['fundamental_summaries'])
            }

        main_doc_path = self._create_main_report(company_analyses)
        if main_doc_path:
            self._upload_to_drive(main_doc_path)
            self._generate_delta_report(main_doc_path)
        
        self._generate_market_events_report(new_fundamental_analyses)

if __name__ == "__main__":
    db_conn = None
    try:
        if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DRIVE_FOLDER_ID, GSPREAD_SERVICE_ACCOUNT_JSON]):
            logging.error("Des secrets essentiels sont manquants. Arrêt du script.")
        else:
            db_conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            report_generator = ComprehensiveReportGenerator(db_conn)
            report_generator.generate_all_reports([])
    except Exception as e:
        logging.error(f"❌ Erreur fatale dans le générateur de rapports : {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()
