# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR (V2.2 - GESTION INTELLIGENTE DES API)
# ==============================================================================

import gspread
from google.oauth2 import service_account
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

class ComprehensiveReportGenerator:
    def __init__(self, spreadsheet_id, drive_folder_id):
        self.spreadsheet_id = spreadsheet_id
        self.drive_folder_id = drive_folder_id
        self.gc = None
        self.gemini_model = None
        self.spreadsheet = None
        self.drive_service = None
        
        # Propriétés pour la gestion intelligente des API
        self.api_keys = []
        self.current_key_index = 0
        self.request_count_per_key = {}
        self.max_requests_per_key = 48
        self.failed_keys = set()
        self.current_key_number = None

    def _load_all_api_keys(self):
        """Charge toutes les clés API disponibles"""
        loaded_keys = []
        for i in range(1, 200):
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                loaded_keys.append((i, key))
                self.request_count_per_key[i] = 0
        
        import random
        random.shuffle(loaded_keys)
        
        self.api_keys = loaded_keys
        logging.info(f"✅ Générateur de rapport: {len(self.api_keys)} clé(s) API chargées")
        return len(self.api_keys) > 0
    
    def _find_next_available_key(self):
        """Trouve la prochaine clé API disponible"""
        starting_index = self.current_key_index
        
        while True:
            if self.current_key_index >= len(self.api_keys):
                self.current_key_index = 0
            
            key_number, key_value = self.api_keys[self.current_key_index]
            
            if (key_number not in self.failed_keys and 
                self.request_count_per_key.get(key_number, 0) < self.max_requests_per_key):
                return key_number, key_value
            
            self.current_key_index += 1
            
            if self.current_key_index == starting_index:
                return None, None

    def _authenticate_google_services(self):
        logging.info("Générateur de rapport: Authentification Google Services...")
        try:
            creds_json_str = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
            if not creds_json_str:
                logging.error("❌ Secret GSPREAD_SERVICE_ACCOUNT introuvable.")
                return False
            creds_dict = json.loads(creds_json_str)
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.gc = gspread.authorize(creds)
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            self.drive_service = build('drive', 'v3', credentials=creds)
            logging.info("✅ Authentification Google Sheets & Drive réussie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur lors de l'authentification : {e}")
            return False

    def _configure_gemini_with_rotation(self):
        """Configure Gemini avec rotation intelligente"""
        if not self._load_all_api_keys():
            logging.error("❌ Générateur: Aucune clé API trouvée")
            return False
        return self._setup_next_key()

    def _setup_next_key(self):
        """Configure la prochaine clé disponible"""
        key_number, key_value = self._find_next_available_key()
        
        if not key_value:
            logging.error("❌ Générateur: Toutes les clés API épuisées")
            return False
        
        try:
            genai.configure(api_key=key_value)
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash-latest')
            self.current_key_number = key_number
            logging.info(f"✅ Générateur: Clé API #{key_number} configurée")
            return True
        except Exception as e:
            logging.warning(f"⚠️ Générateur: Erreur clé #{key_number}: {e}")
            self.failed_keys.add(key_number)
            self.current_key_index += 1
            return self._setup_next_key()

    def _rotate_api_key(self):
        """Rotation vers la prochaine clé"""
        return self._setup_next_key()

    def _call_gemini_with_retry(self, prompt):
        """Appel Gemini avec gestion intelligente des erreurs"""
        if not self.gemini_model:
            return "Erreur : le modèle Gemini n'est pas initialisé."
        
        max_attempts = min(len(self.api_keys), 10)  # Limiter les tentatives
        
        for attempt in range(max_attempts):
            # Vérifier le quota préventif
            current_usage = self.request_count_per_key.get(self.current_key_number, 0)
            if current_usage >= self.max_requests_per_key:
                logging.info(f"Générateur: Quota préventif atteint pour clé #{self.current_key_number}")
                if not self._setup_next_key():
                    return "Erreur : Toutes les clés API ont atteint leur quota"
                continue
            
            try:
                response = self.gemini_model.generate_content(prompt)
                
                # Incrémenter le compteur
                self.request_count_per_key[self.current_key_number] += 1
                
                if hasattr(response, 'text') and response.text:
                    return response.text
                elif response.prompt_feedback:
                    return f"Contenu bloqué : {response.prompt_feedback.block_reason.name}"
                else:
                    return "Erreur : réponse vide de l'API"
                    
            except api_exceptions.ResourceExhausted as e:
                logging.warning(f"Générateur: Quota atteint pour clé #{self.current_key_number}")
                self.failed_keys.add(self.current_key_number)
                if not self._setup_next_key():
                    return "Erreur : Toutes les clés API ont atteint leur quota"
                continue
                
            except Exception as e:
                logging.error(f"Générateur: Erreur inattendue : {e}")
                return f"Erreur technique : {str(e)}"
        
        return "Erreur : Échec après plusieurs tentatives"

    def _upload_to_drive(self, filepath):
        """Version corrigée pour Drive Partagé"""
        try:
            file_metadata = {'name': os.path.basename(filepath), 'parents': [self.drive_folder_id]}
            media = MediaFileUpload(filepath, mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            uploaded_file = self.drive_service.files().create(
                body=file_metadata, 
                media_body=media, 
                fields='id,name', 
                supportsAllDrives=True,
                supportsTeamDrives=True
            ).execute()
            logging.info(f"✅ Fichier '{uploaded_file.get('name')}' sauvegardé sur Google Drive (ID: {uploaded_file.get('id')})")
            return uploaded_file.get('id')
        except Exception as e:
            if "storageQuotaExceeded" in str(e):
                logging.error("❌ ERREUR CRITIQUE : Vous devez utiliser un Drive Partagé (Shared Drive)")
                logging.error("   Solution : Créez un Drive Partagé et utilisez l'ID d'un dossier à l'intérieur")
            else:
                logging.error(f"❌ Erreur lors de la sauvegarde sur Google Drive : {e}")
            return None

    def _find_latest_report_in_drive(self):
        """Version corrigée pour Drive Partagé"""
        logging.info(f"Recherche du dernier rapport dans le dossier Drive ID: {self.drive_folder_id}")
        try:
            query = f"'{self.drive_folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document' and name contains 'Rapport_Synthese_'"
            results = self.drive_service.files().list(
                q=query, 
                pageSize=1, 
                fields="files(id, name)", 
                orderBy="name desc",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                supportsTeamDrives=True
            ).execute()
            items = results.get('files', [])
            if not items:
                logging.info("  -> Aucun rapport précédent trouvé.")
                return None, None
            latest_file = items[0]
            logging.info(f"  -> Dernier rapport trouvé : {latest_file['name']} (ID: {latest_file['id']})")
            return latest_file['id'], latest_file['name']
        except Exception as e:
            logging.error(f"  -> Erreur lors de la recherche sur Drive : {e}")
            return None, None

    def _download_drive_file(self, file_id):
        """Version corrigée pour Drive Partagé"""
        try:
            request = self.drive_service.files().get_media(
                fileId=file_id, 
                supportsAllDrives=True
            )
            file_bytes = BytesIO()
            file_bytes.write(request.execute())
            file_bytes.seek(0)
            return file_bytes
        except Exception as e:
            logging.error(f"  -> Impossible de télécharger le fichier {file_id}: {e}")
            return None

    def _generate_delta_report(self, new_report_path):
        latest_file_id, latest_file_name = self._find_latest_report_in_drive()
        if not latest_file_id: 
            return

        logging.info("Génération du rapport comparatif (delta)...")
        old_file_bytes = self._download_drive_file(latest_file_id)
        if not old_file_bytes: 
            return
        
        try:
            old_doc = Document(old_file_bytes)
            new_doc = Document(new_report_path)
            old_text = "\n".join([p.text for p in old_doc.paragraphs])
            new_text = "\n".join([p.text for p in new_doc.paragraphs])

            prompt = f"""
            Tu es un assistant d'analyse. Compare les deux rapports suivants (ANCIEN et NOUVEAU) et rédige une synthèse des changements les plus significatifs pour un investisseur.
            Concentre-toi sur :
            - Les changements de signaux techniques (ex: Achat -> Neutre).
            - Les nouvelles informations fondamentales importantes.
            - Les changements dans la conclusion générale pour une société.
            Ignore les changements mineurs de formulation. Présente le résultat sous forme de points clés.

            --- ANCIEN RAPPORT ---
            {old_text[:15000]} 
            --- NOUVEAU RAPPORT ---
            {new_text[:15000]}
            """
            delta_summary = self._call_gemini_with_retry(prompt)
            
            delta_doc = Document()
            delta_doc.add_heading(f"Analyse des Changements entre Rapports", level=0)
            delta_doc.add_paragraph(f"Comparaison entre le rapport du jour et le rapport précédent ('{latest_file_name}').")
            delta_doc.add_heading("Synthèse des Changements Significatifs", level=1)
            delta_doc.add_paragraph(delta_summary)
            
            output_filename = f"Rapport_Comparatif_{time.strftime('%Y%m%d_%H%M')}.docx"
            delta_doc.save(output_filename)
            logging.info(f"🎉 Rapport comparatif généré : {output_filename}")
            self._upload_to_drive(output_filename)
        except Exception as e:
            logging.error(f"Erreur lors de la génération du rapport delta : {e}")

    def _generate_market_events_report(self, new_analyses):
        if not new_analyses:
            logging.info("Aucun nouvel événement fondamental à signaler.")
            return
        logging.info("Génération du rapport des événements marquants...")
        prompt = f"""
        Tu es un journaliste financier pour un grand média. Rédige un court article de synthèse sur les événements marquants du jour sur le marché de la BRVM, basé sur les nouvelles analyses de rapports de sociétés suivantes.
        Nouvelles analyses du jour :
        {'---'.join(new_analyses)}
        Structure ton article ainsi :
        1. Un titre accrocheur.
        2. Un paragraphe d'introduction.
        3. Des points clés pour les 2 ou 3 annonces les plus importantes (résultats, dividendes, perspectives...).
        4. Un court paragraphe de conclusion.
        """
        market_summary = self._call_gemini_with_retry(prompt)
        
        events_doc = Document()
        events_doc.add_heading("Synthèse des Événements Marquants du Marché", level=0)
        events_doc.add_paragraph(f"Basé sur les rapports analysés le {time.strftime('%Y-%m-%d')}")
        events_doc.add_paragraph(market_summary)
        
        output_filename = f"Synthese_Marche_{time.strftime('%Y%m%d_%H%M')}.docx"
        events_doc.save(output_filename)
        logging.info(f"🎉 Rapport des événements marquants généré : {output_filename}")
        self._upload_to_drive(output_filename)

    def _get_sheet_data(self, sheet_name):
        logging.info(f"  -> Récupération des données pour {sheet_name}...")
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            data = worksheet.get_all_values()
            if len(data) < 2: 
                return None
            headers = data[0]
            df = pd.DataFrame(data[1:], columns=headers)
            return df
        except Exception as e:
            logging.error(f"  -> Impossible de récupérer les données pour {sheet_name}: {e}")
            return None

    def _find_column_by_keywords(self, columns, keywords):
        for col in columns:
            for keyword in keywords:
                if keyword.lower() in col.lower():
                    return col
        return None

    def _analyze_price_evolution(self, df_prices, date_col, price_col):
        data_string = df_prices[[date_col, price_col]].to_csv(index=False)
        prompt = f"""
        Tu es un analyste de marché financier spécialisé sur la BRVM.
        Analyse l'évolution du cours de l'action sur la période de cotation fournie ci-dessous.
        Données du cours:
        {data_string}
        Fournis une analyse concise en français qui inclut :
        1. Une phrase d'introduction sur la tendance générale (haussière, baissière, stable, volatile).
        2. Les chiffres clés : Cours de début, Cours de fin, Évolution en pourcentage, Point le plus haut, Point le plus bas.
        3. Un bref commentaire sur la dynamique récente.
        """
        return self._call_gemini_with_retry(prompt)

    def _analyze_technical_indicators(self, df_indicators):
        prompt = f"""
        Tu es un analyste technique expert. Analyse les 5 indicateurs techniques pour une action de la BRVM sur les 50 derniers jours, en te concentrant sur les valeurs les plus récentes pour déduire un signal.
        Données des indicateurs :
        {df_indicators.to_csv(index=False)}
        Pour chacun des 5 indicateurs suivants, fournis une analyse de 2-3 phrases et conclus par un signal clair (`Achat`, `Vente`, `Achat Fort`, `Vente Forte`, ou `Neutre`).
        1.  **Moyennes Mobiles (MM5, MM10, MM20, MM50)** : Analyse la position du cours par rapport aux moyennes et le croisement des moyennes.
        2.  **Bandes de Bollinger (Bande_Inferieure, Bande_Supérieure)** : Le cours touche-t-il une des bandes ? La volatilité (écartement des bandes) augmente-t-elle ?
        3.  **MACD (Ligne MACD, Ligne de signal, Histogramme)** : Y a-t-il un croisement récent ? L'histogramme est-il positif ou négatif et quelle est sa dynamique ?
        4.  **RSI** : L'action est-elle en zone de surachat (>70), de survente (<30) ou neutre ?
        5.  **Stochastique (%K, %D)** : Y a-t-il eu un croisement récent dans les zones de surachat (>80) ou de survente (<20) ?
        """
        return self._call_gemini_with_retry(prompt)
            
    def _summarize_fundamental_analysis(self, fundamental_data):
        if not fundamental_data or not fundamental_data.get('rapports_analyses'):
            return "Aucune analyse fondamentale disponible pour cette société."
        reports_text = ""
        for rapport in fundamental_data['rapports_analyses']:
            reports_text += f"--- Titre du rapport: {rapport['titre']} (Date: {rapport['date']}) ---\n"
            reports_text += f"Analyse IA: {rapport['analyse_ia']}\n\n"
        prompt = f"""
        Tu es un analyste financier senior. Synthétise les analyses de rapports financiers ci-dessous pour en extraire les points les plus importants pour un investisseur.
        Analyses existantes :
        {reports_text}
        Rédige un résumé en 3 ou 4 points clés, en mettant l'accent sur les informations les plus récentes (chiffre d'affaires, résultat net, politique de dividende et perspectives).
        """
        return self._call_gemini_with_retry(prompt)

    def get_usage_stats(self):
        """Statistiques d'utilisation des API"""
        total_requests = sum(self.request_count_per_key.values())
        active_keys = len([k for k in self.request_count_per_key.keys() if k not in self.failed_keys])
        failed_keys_count = len(self.failed_keys)
        
        return {
            'total_keys': len(self.api_keys),
            'active_keys': active_keys,
            'failed_keys': failed_keys_count,
            'total_requests': total_requests,
            'current_key': self.current_key_number
        }

    def generate_report(self, fundamental_results, new_fundamental_analyses):
        if not self._authenticate_google_services() or not self._configure_gemini_with_rotation():
            logging.error("Arrêt du générateur de rapport en raison d'un problème d'initialisation.")
            return

        all_sheets = [ws.title for ws in self.spreadsheet.worksheets() if ws.title not in ["UNMATCHED", "Actions_BRVM", "ANALYSIS_MEMORY"]]
        company_reports = {}

        for sheet_name in all_sheets:
            logging.info(f"--- Génération de l'analyse pour : {sheet_name} ---")
            df = self._get_sheet_data(sheet_name)
            if df is None or df.empty:
                logging.warning(f"  -> Aucune donnée pour {sheet_name}, feuille ignorée.")
                continue
            date_col = self._find_column_by_keywords(df.columns, ['date'])
            price_col = self._find_column_by_keywords(df.columns, ['cours', 'prix'])
            if not date_col or not price_col:
                logging.error(f"  -> Colonnes 'Date' ou 'Cours' introuvables pour {sheet_name}. Feuille ignorée.")
                continue
            df[price_col] = pd.to_numeric(df[price_
