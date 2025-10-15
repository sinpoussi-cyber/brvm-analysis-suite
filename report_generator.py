# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR V7.3 - 33 CLÉS API GEMINI V2BETA
# 100 JOURS + PRÉDICTIONS
# ==============================================================================

import psycopg2
import pandas as pd
import os
import time
import logging
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
import requests
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION GEMINI CORRIGÉE - Basé sur vos versions disponibles
GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_API_VERSION = "v2beta"  # Compatible avec: v1, v2, v2beta, v2internal, v3, v3beta
REQUESTS_PER_MINUTE_LIMIT = 15

class ComprehensiveReportGenerator:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.api_keys = []
        self.current_key_index = 0
        self.request_timestamps = []

    def _configure_api_keys(self):
        """Charge les 15 premières clés pour les rapports (sur 33 disponibles)"""
        for i in range(1, 16):  # Utiliser les 15 premières clés
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key: 
                self.api_keys.append(key)
        
        if not self.api_keys:
            logging.warning("⚠️  Aucune clé API Gemini trouvée.")
            return False
        
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées (sur 33 disponibles).")
        logging.info(f"📝 Modèle: {GEMINI_MODEL} | API Version: {GEMINI_API_VERSION}")
        return True

    def _call_gemini_with_retry(self, prompt):
        """Appelle l'API Gemini avec gestion des erreurs et retry"""
        if not self.api_keys:
            return "Analyse IA non disponible (aucune clé API configurée)."

        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        if len(self.request_timestamps) >= REQUESTS_PER_MINUTE_LIMIT:
            sleep_time = 60 - (now - self.request_timestamps[0]) if self.request_timestamps else 60
            logging.warning(f"⏸️  Pause rate limit: {sleep_time + 1:.1f}s")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []

        while self.current_key_index < len(self.api_keys):
            api_key = self.api_keys[self.current_key_index]
            # ✅ URL CORRIGÉE avec v2beta
            api_url = f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models/{GEMINI_MODEL}:generateContent?key={api_key}"
            
            try:
                self.request_timestamps.append(time.time())
                
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
                    logging.warning(f"⚠️  Quota atteint pour clé #{self.current_key_index + 1}")
                    self.current_key_index += 1
                    continue
                
                response.raise_for_status()
                response_json = response.json()
                return response_json['candidates'][0]['content']['parts'][0]['text']
                
            except Exception as e:
                logging.error(f"❌ Erreur clé #{self.current_key_index + 1}: {e}")
                self.current_key_index += 1
        
        return "Erreur d'analyse : Quota API épuisé."

    def _get_all_data_from_db(self):
        """Récupère les données sur 100 jours depuis Supabase"""
        logging.info("📂 Récupération des données (100 derniers jours)...")
        
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
        WHERE lhd.rn <= 100 OR lhd.rn IS NULL;
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
        
        logging.info(f"   ✅ Données (100 jours) pour {len(company_data)} sociétés récupérées")
        return company_data

    def _get_predictions_from_db(self, symbol):
        """Récupère les prédictions depuis Supabase"""
        try:
            query = """
            SELECT 
                p.prediction_date,
                p.predicted_price,
                p.lower_bound,
                p.upper_bound,
                p.confidence_level
            FROM predictions p
            JOIN companies c ON p.company_id = c.id
            WHERE c.symbol = %s
            ORDER BY p.prediction_date
            """
            df = pd.read_sql(query, self.db_conn, params=(symbol,))
            
            if df.empty:
                return None
            
            return df
        except Exception as e:
            logging.error(f"❌ Erreur récupération prédictions {symbol}: {e}")
            return None

    def _analyze_price_evolution(self, df_prices):
        """Analyse l'évolution du cours sur 100 jours avec IA"""
        if df_prices.empty or df_prices['price'].isnull().all():
            return "Données de prix insuffisantes."
        
        data_string = df_prices.to_csv(index=False)
        prompt = f"""Analyse l'évolution du cours de cette action sur les 100 derniers jours. 

Fournis une analyse structurée avec :
- **Tendance générale** (haussière, baissière, stable) sur les 100 jours
- **Chiffres clés** :
  - Prix début de période vs fin de période
  - Variation en % sur les 100 jours
  - Plus haut et plus bas atteints
  - Volatilité observée
- **Phases marquantes** : Identifie 2-3 phases distinctes si présentes
- **Dynamique récente** (30 derniers jours)

Données (100 jours):
{data_string}"""
        
        return self._call_gemini_with_retry(prompt)

    def _analyze_predictions(self, df_predictions, current_price):
        """Analyse les prédictions avec IA"""
        if df_predictions is None or df_predictions.empty:
            return "Aucune prédiction disponible."
        
        first_pred = df_predictions.iloc[0]['predicted_price']
        last_pred = df_predictions.iloc[-1]['predicted_price']
        avg_pred = df_predictions['predicted_price'].mean()
        
        change_percent = ((last_pred - current_price) / current_price * 100)
        
        data_string = df_predictions.to_csv(index=False)
        
        prompt = f"""Analyse ces prédictions de prix pour les 20 prochains jours ouvrables (Lundi-Vendredi).

Prix actuel: {current_price:.2f} F CFA
Prix prédit à J+20: {last_pred:.2f} F CFA
Variation prévue: {change_percent:.2f}%

Fournis une analyse concise avec :
- **Tendance prévue** (haussière, baissière, stable)
- **Points clés** : Variation attendue, fourchette de prix, moments critiques
- **Niveau de confiance** de la prédiction
- **Recommandation** pour un investisseur (court terme)

Données des prédictions:
{data_string}"""
        
        return self._call_gemini_with_retry(prompt)

    def _analyze_technical_indicators(self, series_indicators):
        """Analyse les indicateurs techniques avec IA"""
        data_string = series_indicators.to_string()
        prompt = f"""Analyse ces indicateurs techniques (jour le plus récent).

Pour chaque indicateur, fournis :
- Une analyse concise (2-3 phrases)
- Un signal clair (Achat/Vente/Neutre)

Indicateurs:
{data_string}"""
        
        return self._call_gemini_with_retry(prompt)

    def _summarize_fundamental_analysis(self, summaries):
        """Synthétise les analyses fondamentales avec IA"""
        prompt = f"""Synthétise ces analyses fondamentales en 3-4 points clés.

Concentre-toi sur :
- Chiffre d'affaires
- Résultat net
- Dividendes
- Perspectives

Analyses:
{summaries}"""
        
        return self._call_gemini_with_retry(prompt)

    def _create_main_report(self, company_analyses):
        """Génère le rapport Word avec prédictions"""
        logging.info("📝 Création du rapport de synthèse (100 jours + prédictions)...")
        
        doc = Document()
        
        # Titre
        title = doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Métadonnées
        meta = doc.add_paragraph()
        meta.add_run(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}\n").bold = True
        meta.add_run(f"Propulsé par {GEMINI_MODEL} (API {GEMINI_API_VERSION}) | Analyse sur 100 jours | Prédictions 20 jours ouvrables (Lun-Ven)\n")
        meta.add_run(f"Base de données : Supabase (PostgreSQL) | Version : 7.3")
        meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        doc.add_paragraph()
        
        # Contenu pour chaque société
        for symbol, analyses in sorted(company_analyses.items()):
            nom_societe = analyses.get('nom_societe', symbol)
            
            heading = doc.add_heading(f'{nom_societe} ({symbol})', level=1)
            heading.runs[0].font.color.rgb = RGBColor(0, 51, 102)
            
            # 1. Évolution du Cours (100 jours)
            doc.add_heading('1. Évolution du Cours (100 derniers jours)', level=2)
            doc.add_paragraph(analyses.get('price_analysis', 'Analyse non disponible.'))
            
            # 2. Prédictions (20 jours ouvrables)
            if 'predictions_analysis' in analyses:
                doc.add_heading('2. Prédictions (20 prochains jours ouvrables Lun-Ven)', level=2)
                doc.add_paragraph(analyses['predictions_analysis'])
            
            # 3. Analyse Technique
            doc.add_heading('3. Analyse Technique des Indicateurs', level=2)
            doc.add_paragraph(analyses.get('technical_analysis', 'Analyse technique non disponible.'))
            
            # 4. Synthèse Fondamentale
            doc.add_heading('4. Synthèse Fondamentale', level=2)
            doc.add_paragraph(analyses.get('fundamental_summary', 'Analyse fondamentale non disponible.'))
            
            doc.add_page_break()

        # Sauvegarde
        output_filename = f"Rapport_Synthese_Investissement_BRVM_{time.strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_filename)
        
        logging.info(f"🎉 Rapport généré: {output_filename}")
        return output_filename

    def generate_all_reports(self, new_fundamental_analyses):
        """Génère tous les rapports depuis Supabase"""
        logging.info("="*80)
        logging.info("📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V7.3 - 33 CLÉS DISPONIBLES)")
        logging.info("="*80)

        if not self._configure_api_keys():
            logging.warning("⚠️  Génération sans clés API Gemini")
            
        all_data = self._get_all_data_from_db()
        company_analyses = {}

        for symbol, data in all_data.items():
            logging.info(f"--- Génération synthèses IA: {symbol} ---")
            
            current_price = data['price_data']['price'].iloc[-1] if not data['price_data'].empty else 0
            
            company_analyses[symbol] = {
                'nom_societe': data['nom_societe'],
                'price_analysis': self._analyze_price_evolution(data['price_data']),
                'technical_analysis': self._analyze_technical_indicators(data['indicator_data']),
                'fundamental_summary': self._summarize_fundamental_analysis(data['fundamental_summaries'])
            }
            
            df_predictions = self._get_predictions_from_db(symbol)
            if df_predictions is not None and not df_predictions.empty:
                company_analyses[symbol]['predictions_analysis'] = self._analyze_predictions(
                    df_predictions, 
                    current_price
                )

        self._create_main_report(company_analyses)
        logging.info("✅ Génération rapports terminée")

if __name__ == "__main__":
    db_conn = None
    try:
        if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
            logging.error("❌ Secrets DB manquants")
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
        logging.error(f"❌ Erreur: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()
