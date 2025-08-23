# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR
# Description: Génère un rapport de synthèse complet en utilisant les données
#              collectées et les analyses fondamentales et techniques.
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
from docx.shared import Pt, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
import google.generativeai as genai

class ComprehensiveReportGenerator:
    def __init__(self, spreadsheet_id, api_key):
        self.spreadsheet_id = spreadsheet_id
        self.api_key = api_key
        self.gc = None
        self.gemini_model = None
        self.spreadsheet = None

    def _authenticate(self):
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
            logging.info("✅ Authentification Google réussie.")
            
            genai.configure(api_key=self.api_key)
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash-latest')
            logging.info("✅ API Gemini configurée.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur lors de l'initialisation : {e}")
            return False

    def _get_sheet_data(self, sheet_name):
        logging.info(f"  -> Récupération des données pour {sheet_name}...")
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            data = worksheet.get_all_values()
            if len(data) < 51:
                logging.warning(f"  -> Moins de 50 jours de données pour {sheet_name}. Utilisation de {len(data)-1} jours.")
                if len(data) < 2: return None
            
            headers = data[0]
            df = pd.DataFrame(data[1:], columns=headers)
            
            # Garder uniquement les 50 dernières lignes
            df = df.tail(50).reset_index(drop=True)
            return df
        except Exception as e:
            logging.error(f"  -> Impossible de récupérer les données pour {sheet_name}: {e}")
            return None

    def _analyze_price_evolution(self, df_prices):
        prompt = f"""
        Tu es un analyste de marché financier spécialisé sur la BRVM.
        Analyse l'évolution du cours de l'action sur les 50 derniers jours de cotation fournis ci-dessous.

        Données du cours:
        Date,Cours (F CFA)
        {df_prices.to_csv(index=False)}

        Fournis une analyse concise en français qui inclut :
        1. Une phrase d'introduction sur la tendance générale (haussière, baissière, stable, volatile).
        2. Les chiffres clés : Cours de début, Cours de fin, Évolution en pourcentage, Point le plus haut, Point le plus bas.
        3. Un bref commentaire sur la dynamique récente.
        """
        try:
            response = self.gemini_model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Erreur lors de l'analyse de l'évolution du cours : {e}"

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
        try:
            response = self.gemini_model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Erreur lors de l'analyse des indicateurs techniques : {e}"
            
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
        try:
            response = self.gemini_model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Erreur lors de la synthèse fondamentale : {e}"

    def generate_report(self, fundamental_results):
        if not self._authenticate():
            return

        all_sheets = [ws.title for ws in self.spreadsheet.worksheets() if ws.title not in ["UNMATCHED", "Actions_BRVM"]]
        company_reports = {}

        for sheet_name in all_sheets:
            logging.info(f"--- Génération de l'analyse pour : {sheet_name} ---")
            df = self._get_sheet_data(sheet_name)
            if df is None or df.empty:
                continue

            # 1. Analyse de l'évolution du cours
            price_cols = ['Date', 'Cours (F CFA)']
            df_prices = df.loc[:, price_cols].copy()
            price_analysis = self._analyze_price_evolution(df_prices)
            time.sleep(2) # Pause pour l'API

            # 2. Analyse des indicateurs techniques
            indicator_cols = ['MM5', 'MM10', 'MM20', 'MM50', 'Bande_centrale', 'Bande_Inferieure', 'Bande_Supérieure', 'Ligne MACD', 'Ligne de signal', 'Histogramme', 'RSI', '%K', '%D']
            df_indicators = df.loc[:, indicator_cols].copy()
            technical_analysis = self._analyze_technical_indicators(df_indicators)
            time.sleep(2)

            # 3. Synthèse de l'analyse fondamentale
            fundamental_data = fundamental_results.get(sheet_name, {})
            fundamental_summary = self._summarize_fundamental_analysis(fundamental_data)
            
            company_reports[sheet_name] = {
                'price_analysis': price_analysis,
                'technical_analysis': technical_analysis,
                'fundamental_summary': fundamental_summary,
                'nom_societe': fundamental_data.get('nom', sheet_name)
            }
            logging.info(f"  -> Analyses pour {sheet_name} terminées.")

        # 4. Génération du document Word
        self._create_word_report(company_reports)

    def _create_word_report(self, company_reports):
        logging.info("Création du rapport de synthèse final...")
        doc = Document()
        doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        
        # --- Page de résumé global ---
        doc.add_heading('Synthèse Globale du Marché', level=1)
        
        global_summary_text = "Voici un aperçu des analyses individuelles :\n\n"
        for symbol, reports in company_reports.items():
            global_summary_text += f"**{reports['nom_societe']} ({symbol})**\n"
            global_summary_text += f"- Tendance du cours: {reports['price_analysis'].splitlines()[0]}\n"
            global_summary_text += f"- Fondamentaux: {reports['fundamental_summary'].splitlines()[0] if reports['fundamental_summary'] != 'Aucune analyse fondamentale disponible pour cette société.' else 'Non disponible.'}\n\n"
            
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
        try:
            global_summary = self.gemini_model.generate_content(prompt_global).text
        except Exception as e:
            global_summary = f"Impossible de générer la synthèse globale : {e}"

        doc.add_paragraph(global_summary)
        doc.add_page_break()

        # --- Pages individuelles pour chaque société ---
        for symbol, reports in company_reports.items():
            nom_societe = reports['nom_societe']
            doc.add_heading(f'Analyse Détaillée : {nom_societe} ({symbol})', level=1)
            
            doc.add_heading('1. Évolution du Cours (50 derniers jours)', level=2)
            doc.add_paragraph(reports['price_analysis'])
            
            doc.add_heading('2. Analyse Technique des Indicateurs', level=2)
            doc.add_paragraph(reports['technical_analysis'])
            
            doc.add_heading('3. Synthèse Fondamentale', level=2)
            doc.add_paragraph(reports['fundamental_summary'])

            # Conclusion par IA
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
            try:
                conclusion = self.gemini_model.generate_content(prompt_conclusion).text
            except Exception as e:
                conclusion = f"Erreur lors de la génération de la conclusion : {e}"
            doc.add_paragraph(conclusion)
            doc.add_page_break()

        output_filename = f"Rapport_Synthese_Investissement_BRVM_{time.strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_filename)
        logging.info(f"🎉 Rapport de synthèse final généré : {output_filename}")
