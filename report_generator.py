# ==============================================================================
# FICHIER 1: report_generator.py (V5.0 - GEMINI 2.0 FLASH)
# ==============================================================================

import psycopg2
import pandas as pd
import os
import json
import time
import logging
from docx import Document
import requests
from collections import defaultdict
from datetime import datetime

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# Configuration pour Gemini 2.0 Flash
GEMINI_MODEL = "gemini-2.0-flash-exp"
REQUESTS_PER_MINUTE_LIMIT = 15  # Limite pour Gemini 2.0 Flash

class ComprehensiveReportGenerator:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.api_keys = []
        self.current_key_index = 0
        self.request_timestamps = []

    def _configure_api_keys(self):
        for i in range(1, 20):
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key: 
                self.api_keys.append(key)
        
        if not self.api_keys:
            logging.warning("⚠️ Aucune clé API Gemini trouvée. Les analyses IA seront vides.")
            return False
        
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini 2.0 Flash chargées.")
        return True

    def _call_gemini_with_retry(self, prompt):
        if not self.api_keys:
            return "Analyse IA non disponible (aucune clé API configurée)."

        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        if len(self.request_timestamps) >= REQUESTS_PER_MINUTE_LIMIT:
            sleep_time = 60 - (now - self.request_timestamps[0]) if self.request_timestamps else 60
            logging.warning(f"Limite de requêtes/minute atteinte. Pause de {sleep_time + 1:.1f} secondes...")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []

        while self.current_key_index < len(self.api_keys):
            api_key = self.api_keys[self.current_key_index]
            api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={api_key}"
            
            try:
                self.request_timestamps.append(time.time())
                
                # Configuration optimisée pour Gemini 2.0 Flash
                request_body = {
                    "contents": [{
                        "parts": [{"text": prompt}]
                    }],
                    "generationConfig": {
                        "temperature": 0.7,
                        "topK": 40,
                        "topP": 0.95,
                        "maxOutputTokens": 2048,
                    }
                }
                
                response = requests.post(api_url, json=request_body, timeout=90)

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
        
        return "Erreur d'analyse : Le quota de toutes les clés API a probablement été atteint."

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
            ta.mm_decision, ta.bollinger_decision, ta.macd_decision,
            ta.rsi_decision, ta.stochastic_decision,
            (SELECT STRING_AGG(fa.analysis_summary, E'\\n---\\n' ORDER BY fa.report_date DESC) 
             FROM fundamental_analysis fa 
             WHERE fa.company_id = c.id) as fundamental_summaries
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
        prompt = f"""Analyse l'évolution du cours de cette action sur les 50 derniers jours. 
        
Fournis :
- Tendance générale (haussière, baissière, stable)
- Chiffres clés (début, fin, variation en %, plus haut, plus bas)
- Dynamique récente

Données:
{data_string}"""
        
        return self._call_gemini_with_retry(prompt)

    def _analyze_technical_indicators(self, series_indicators):
        data_string = series_indicators.to_string()
        prompt = f"""Analyse ces indicateurs techniques pour le jour le plus récent. 

Pour chaque indicateur (Moyennes Mobiles, Bollinger, MACD, RSI, Stochastique) :
- Donne une analyse de 2-3 phrases
- Fournis un signal clair (Achat, Vente, Neutre)

Indicateurs:
{data_string}"""
        
        return self._call_gemini_with_retry(prompt)

    def _summarize_fundamental_analysis(self, summaries):
        prompt = f"""Synthétise ces analyses de rapports financiers en 3 ou 4 points clés pour un investisseur.

Concentre-toi sur :
- Chiffre d'affaires
- Résultat net
- Dividendes
- Perspectives

Analyses:
{summaries}"""
        
        return self._call_gemini_with_retry(prompt)

    def _create_main_report(self, company_analyses):
        logging.info("Création du rapport de synthèse principal...")
        
        doc = Document()
        doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        doc.add_paragraph(f"Généré le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        doc.add_paragraph(f"Propulsé par Gemini {GEMINI_MODEL}")
        
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

    def generate_all_reports(self, new_fundamental_analyses):
        logging.info("="*60)
        logging.info("ÉTAPE 4 : GÉNÉRATION DES RAPPORTS (GEMINI 2.0 FLASH)")
        logging.info("="*60)

        if not self._configure_api_keys():
            logging.warning("Génération des rapports sans clés API Gemini.")
            
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

        self._create_main_report(company_analyses)
        logging.info("✅ Génération de tous les rapports terminée.")

if __name__ == "__main__":
    db_conn = None
    try:
        if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
            logging.error("Des secrets de base de données sont manquants. Arrêt du script.")
        else:
            db_conn = psycopg2.connect(
                dbname=DB_NAME, 
                user=DB_USER, 
                password=DB_PASSWORD, 
                host=DB_HOST, 
                port=DB_PORT
            )
            report_generator = ComprehensiveReportGenerator(db_conn)
            report_generator.generate_all_reports([])
    except Exception as e:
        logging.error(f"❌ Erreur fatale dans le générateur de rapports : {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()


# ==============================================================================
# FICHIER 2: fundamental_analyzer.py (V5.0 - GEMINI 2.0 FLASH)
# ==============================================================================
# Section critique à modifier dans fundamental_analyzer.py

# Ligne 18 - Ajouter la configuration du modèle
GEMINI_MODEL = "gemini-2.0-flash-exp"
REQUESTS_PER_MINUTE_LIMIT = 15  # Limite pour Gemini 2.0 Flash

# Dans la classe BRVMAnalyzer, méthode _analyze_pdf_with_direct_api
# Ligne ~140 - Remplacer l'URL de l'API par :

def _analyze_pdf_with_direct_api(self, company_id, symbol, report):
    # ... (code existant jusqu'à la définition de api_url) ...
    
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={api_key}"
    
    try:
        logging.info(f"    -> Nouvelle analyse IA Gemini 2.0 Flash : {os.path.basename(pdf_url)}")
        
        # ... (lecture du PDF) ...
        
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

        request_body = {
            "contents": [{
                "parts": [
                    {"text": prompt}, 
                    {
                        "inline_data": {
                            "mime_type": "application/pdf", 
                            "data": pdf_data
                        }
                    }
                ]
            }],
            "generationConfig": {
                "temperature": 0.7,
                "topK": 40,
                "topP": 0.95,
                "maxOutputTokens": 2048,
            }
        }

        self.request_timestamps.append(time.time())
        response = requests.post(api_url, json=request_body, timeout=120)  # Timeout augmenté

        # ... (reste du code de gestion des erreurs) ...
