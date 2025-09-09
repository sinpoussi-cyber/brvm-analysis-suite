# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR (V1.2 - ROTATION DE CLÉS API)
# Description: Génère un rapport de synthèse complet et gère plusieurs clés API
#              pour contourner les limites de quota journalières.
# ==============================================================================

import gspread
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import os
import json
import time
import logging
from docx import Document
from docx.shared import Pt
import google.generativeai as genai
from google.api_core import exceptions as api_exceptions

class ComprehensiveReportGenerator:
    def __init__(self, spreadsheet_id):
        self.spreadsheet_id = spreadsheet_id
        self.gc = None
        self.gemini_model = None
        self.spreadsheet = None
        # NOUVEAU : Attributs pour la gestion des clés API
        self.api_keys = []
        self.current_key_index = 0

    def _authenticate_gsheets(self):
        logging.info("Générateur de rapport: Authentification Google Services...")
        try:
            creds_json_str = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
            if not creds_json_str:
                logging.error("❌ Secret GSPREAD_SERVICE_ACCOUNT introuvable.")
                return False
            creds_dict = json.loads(creds_json_str)
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.gc = gspread.authorize(creds)
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            logging.info("✅ Authentification Google Sheets réussie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur lors de l'authentification GSheets : {e}")
            return False

    def _configure_gemini_with_rotation(self):
        """Charge toutes les clés API Gemini depuis les variables d'environnement."""
        for i in range(1, 10):
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                self.api_keys.append(key)

        if not self.api_keys:
            logging.error("❌ Aucune clé API nommée 'GOOGLE_API_KEY_n' n'a été trouvée.")
            return False
            
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées pour le générateur de rapport.")
        
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

    def _call_gemini_with_retry(self, prompt):
        """Appelle l'API Gemini et gère la rotation des clés en cas d'erreur de quota."""
        if not self.gemini_model:
            return "Erreur : le modèle Gemini n'est pas initialisé."
        
        max_retries = len(self.api_keys)
        for attempt in range(max_retries):
            try:
                response = self.gemini_model.generate_content(prompt)
                return response.text
            except api_exceptions.ResourceExhausted as e:
                logging.warning(f"Quota atteint pour la clé API #{self.current_key_index + 1}. ({e})")
                if not self._rotate_api_key():
                    return "Erreur d'analyse : Toutes les clés API ont atteint leur quota."
            except Exception as e:
                logging.error(f"Erreur inattendue lors de l'appel à Gemini : {e}")
                return f"Erreur technique lors de l'appel à l'IA : {e}"
        
        return "Erreur d'analyse : Échec après avoir essayé toutes les clés API."

    def _get_sheet_data(self, sheet_name):
        logging.info(f"  -> Récupération des données pour {sheet_name}...")
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            data = worksheet.get_all_values()
            if len(data) < 2:
                logging.warning(f"  -> Pas de données ou seulement un en-tête pour {sheet_name}.")
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

    def generate_report(self, fundamental_results):
        if not self._authenticate_gsheets() or not self._configure_gemini_with_rotation():
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

            df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
            df.dropna(subset=[price_col], inplace=True)
            df = df.tail(50).reset_index(drop=True)
            
            if df.empty:
                logging.warning(f"  -> Pas de données valides après nettoyage pour {sheet_name}. Feuille ignorée.")
                continue

            price_analysis = self._analyze_price_evolution(df, date_col, price_col)
            
            indicator_cols = ['MM5', 'MM10', 'MM20', 'MM50', 'Bande_centrale', 'Bande_Inferieure', 'Bande_Supérieure', 'Ligne MACD', 'Ligne de signal', 'Histogramme', 'RSI', '%K', '%D']
            df_indicators = df.loc[:, indicator_cols].copy()
            technical_analysis = self._analyze_technical_indicators(df_indicators)
            
            fundamental_data = fundamental_results.get(sheet_name, {})
            fundamental_summary = self._summarize_fundamental_analysis(fundamental_data)
            
            company_reports[sheet_name] = {
                'price_analysis': price_analysis,
                'technical_analysis': technical_analysis,
                'fundamental_summary': fundamental_summary,
                'nom_societe': fundamental_data.get('nom', sheet_name)
            }
            logging.info(f"  -> Analyses pour {sheet_name} terminées.")

        if not company_reports:
            logging.error("Aucune donnée n'a pu être analysée. Le rapport final ne sera pas généré.")
            return
            
        self._create_word_report(company_reports)

    def _create_word_report(self, company_reports):
        logging.info("Création du rapport de synthèse final...")
        doc = Document()
        doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        
        doc.add_heading('Synthèse Globale du Marché', level=1)
        
        global_summary_text = "Voici un aperçu des analyses individuelles :\n\n"
        for symbol, reports in company_reports.items():
            global_summary_text += f"**{reports['nom_societe']} ({symbol})**\n"
            price_first_line = reports['price_analysis'].splitlines()[0] if reports['price_analysis'] else "Non disponible."
            fundamental_first_line = reports['fundamental_summary'].splitlines()[0] if reports['fundamental_summary'] and "Aucune analyse" not in reports['fundamental_summary'] else 'Non disponible.'
            global_summary_text += f"- Tendance du cours: {price_first_line}\n"
            global_summary_text += f"- Fondamentaux: {fundamental_first_line}\n\n"
            
        prompt_global = f"""
        Tu es le directeur de la recherche d'une banque d'investissement. Rédige une synthèse exécutive (un "executive summary") pour un rapport de marché sur la BRVM, basé sur les résumés individuels suivants.

        Données:
        {global_summary_text}

        La synthèse doit inclure:
        1. Un paragraphe sur le sentiment général du marché.
        2. Une liste à puces "Actions à Surveiller (Signaux Positifs)" avec une justification d'une ligne pour chacune.
        3. Une liste à puces "Actions à Considérer avec Prudence" avec une justification d'une ligne pour chacune.
        Sois concis et professionnel.
        """
        global_summary = self._call_gemini_with_retry(prompt_global)
        doc.add_paragraph(global_summary)
        doc.add_page_break()

        for symbol, reports in company_reports.items():
            nom_societe = reports['nom_societe']
            doc.add_heading(f'Analyse Détaillée : {nom_societe} ({symbol})', level=1)
            
            doc.add_heading('1. Évolution du Cours (50 derniers jours)', level=2)
            doc.add_paragraph(reports['price_analysis'])
            
            doc.add_heading('2. Analyse Technique des Indicateurs', level=2)
            doc.add_paragraph(reports['technical_analysis'])
            
            doc.add_heading('3. Synthèse Fondamentale', level=2)
            doc.add_paragraph(reports['fundamental_summary'])

            doc.add_heading('4. Conclusion d\'Investissement', level=2)
            prompt_conclusion = f"""
            Synthétise les trois analyses suivantes (évolution du cours, indicateurs techniques, et fondamentaux) pour {nom_societe} en une conclusion d'investissement finale.
            
            Analyse du Cours:
            {reports['price_analysis']}
            
            Analyse Technique:
            {reports['technical_analysis']}
            
            Analyse Fondamentale:
            {reports['fundamental_summary']}
            
            Rédige un paragraphe de conclusion qui combine les signaux techniques, la tendance du cours et la santé financière de l'entreprise pour donner un avis global et nuancé.
            """
            conclusion = self._call_gemini_with_retry(prompt_conclusion)
            doc.add_paragraph(conclusion)
            doc.add_page_break()

        output_filename = f"Rapport_Synthese_Investissement_BRVM_{time.strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_filename)
        logging.info(f"🎉 Rapport de synthèse final généré : {output_filename}")
