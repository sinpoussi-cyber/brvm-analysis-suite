# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR V9.0 - GESTION INTELLIGENTE
# ==============================================================================

import psycopg2
import pandas as pd
import os
import time
import logging
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import requests
from collections import defaultdict
from datetime import datetime

# Import du gestionnaire de clés API
from api_key_manager import APIKeyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION GEMINI
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash-latest")
GEMINI_API_VERSION = os.environ.get("GEMINI_API_VERSION", "v1beta")


def _build_gemini_url(model: str) -> str:
    """Construit l'URL d'appel à l'API Gemini pour le modèle fourni."""

    clean_model = model.strip()
    return (
        f"https://generativelanguage.googleapis.com/"
        f"{GEMINI_API_VERSION}/models/{clean_model}:generateContent"
    )
    
class ComprehensiveReportGenerator:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        
        # ✅ Gestionnaire de clés API
        self.api_manager = APIKeyManager('report_generator')
        self.current_api_key = None

    def _get_next_api_key(self):
        """Obtient la prochaine clé API disponible"""
        key_info = self.api_manager.get_next_key()
        if key_info:
            self.current_api_key = key_info
            return key_info['key']
        return None

    def _call_gemini_with_retry(self, prompt):
        """Appelle l'API Gemini avec gestion intelligente des clés"""
        
        # Stats initiales
        available_keys = self.api_manager.get_available_keys()
        
        if not available_keys:
            logging.error("❌ Aucune clé API disponible pour report_generator")
            return "Analyse IA non disponible (toutes les clés épuisées)."
        
        max_attempts = len(available_keys)
        attempts = 0
        
        while attempts < max_attempts:
            api_key = self._get_next_api_key()
            
            if not api_key:
                return "Erreur d'analyse : Toutes les clés API ont échoué."
            
            key_num = self.current_api_key['number']
            
            # Gestion rate limit
            self.api_manager.handle_rate_limit()
            
            api_url =_build_gemini_url(GEMINI_MODEL)
            
            headers = {
                "Content-Type": "application/json",
                "x-goog-api-key": api_key
            }
            
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
            
            try:
                response = requests.post(api_url, headers=headers, json=request_body, timeout=90)

                if response.status_code == 200:
                    response_json = response.json()
                    return response_json['candidates'][0]['content']['parts'][0]['text']
                
                elif response.status_code == 429:
                    logging.warning(f"⚠️  Quota épuisé pour clé #{key_num}")
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                elif response.status_code == 404:
                    error_detail = ""
                    try:
                        error_detail = response.json().get("error", {}).get("message", "")
                    except ValueError:
                        error_detail = response.text[:200]

                    logging.error(
                        "❌ 404 avec clé #%s - %s",
                        key_num,
                        error_detail or "Endpoint ou modèle introuvable",
                    )
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                elif response.status_code == 403:
                    logging.error(f"❌ 403 avec clé #{key_num}")
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                else:
                    logging.error(f"❌ Erreur {response.status_code} avec clé #{key_num}")
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                    
            except requests.exceptions.Timeout:
                logging.error(f"❌ Timeout clé #{key_num}")
                self.api_manager.move_to_next_key()
                attempts += 1
            except Exception as e:
                logging.error(f"❌ Exception clé #{key_num}: {e}")
                self.api_manager.move_to_next_key()
                attempts += 1
        
        return "Erreur d'analyse : Toutes les clés API ont échoué."

    def _get_all_data_from_db(self):
        """Récupère les données sur 100 jours"""
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
        
        logging.info(f"   ✅ Données pour {len(company_data)} sociétés récupérées")
        return company_data

    def _get_predictions_from_db(self, symbol):
        """Récupère TOUTES les 20 prédictions"""
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
        """Analyse l'évolution du cours sur 100 jours"""
        if df_prices.empty or df_prices['price'].isnull().all():
            return "Données de prix insuffisantes."
        
        start_price = df_prices['price'].iloc[0]
        end_price = df_prices['price'].iloc[-1]
        variation = ((end_price - start_price) / start_price * 100)
        max_price = df_prices['price'].max()
        min_price = df_prices['price'].min()
        
        data_summary = f"""Prix début: {start_price:.2f} F CFA
Prix fin: {end_price:.2f} F CFA
Variation: {variation:.2f}%
Plus haut: {max_price:.2f} F CFA
Plus bas: {min_price:.2f} F CFA
Nombre de jours: {len(df_prices)}"""
        
        prompt = f"""Analyse l'évolution du cours de cette action sur les 100 derniers jours. 

Fournis une analyse structurée avec :
- **Tendance générale** (haussière, baissière, stable)
- **Chiffres clés** : Variation, volatilité
- **Phases marquantes** : 2-3 phases distinctes
- **Dynamique récente** (30 derniers jours)

Résumé:
{data_summary}"""
        
        return self._call_gemini_with_retry(prompt)

    def _analyze_predictions_detailed(self, df_predictions, current_price):
        """Analyse détaillée des 20 prédictions jour par jour"""
        if df_predictions is None or df_predictions.empty:
            return "Aucune prédiction disponible."
        
        # Créer un résumé détaillé par groupes de jours
        predictions_by_week = []
        
        # Semaine 1 (J+1 à J+5)
        week1 = df_predictions.iloc[0:5]
        w1_avg = week1['predicted_price'].mean()
        w1_conf = week1['confidence_level'].iloc[0]
        predictions_by_week.append(f"Semaine 1 (J+1 à J+5): Moyenne {w1_avg:.0f} F CFA, Confiance {w1_conf}")
        
        # Semaine 2 (J+6 à J+10)
        week2 = df_predictions.iloc[5:10]
        w2_avg = week2['predicted_price'].mean()
        w2_conf = week2['confidence_level'].iloc[0]
        predictions_by_week.append(f"Semaine 2 (J+6 à J+10): Moyenne {w2_avg:.0f} F CFA, Confiance {w2_conf}")
        
        # Semaines 3-4 (J+11 à J+20)
        week3_4 = df_predictions.iloc[10:20]
        w3_4_avg = week3_4['predicted_price'].mean()
        w3_4_conf = week3_4['confidence_level'].iloc[0]
        predictions_by_week.append(f"Semaines 3-4 (J+11 à J+20): Moyenne {w3_4_avg:.0f} F CFA, Confiance {w3_4_conf}")
        
        predictions_text = "\n".join(predictions_by_week)
        
        # Détails jours clés
        j1 = df_predictions.iloc[0]
        j5 = df_predictions.iloc[4]
        j10 = df_predictions.iloc[9]
        j20 = df_predictions.iloc[19]
        
        key_days = f"""J+1: {j1['predicted_price']:.0f} F CFA ({j1['lower_bound']:.0f}-{j1['upper_bound']:.0f})
J+5: {j5['predicted_price']:.0f} F CFA ({j5['lower_bound']:.0f}-{j5['upper_bound']:.0f})
J+10: {j10['predicted_price']:.0f} F CFA ({j10['lower_bound']:.0f}-{j10['upper_bound']:.0f})
J+20: {j20['predicted_price']:.0f} F CFA ({j20['lower_bound']:.0f}-{j20['upper_bound']:.0f})"""
        
        last_pred = df_predictions.iloc[-1]['predicted_price']
        change_percent = ((last_pred - current_price) / current_price * 100)
        
        prompt = f"""Analyse ces prédictions de prix pour les 20 prochains jours ouvrables.

Prix actuel: {current_price:.2f} F CFA
Variation totale prévue: {change_percent:.2f}%

RÉSUMÉ PAR PÉRIODES:
{predictions_text}

JOURS CLÉS:
{key_days}

Fournis une analyse concise avec :
- **Tendance prévue** sur les 20 jours
- **Évolution par phases** : Identifie les phases (hausse/stabilisation/baisse)
- **Fourchettes de prix** : Prix min/max attendus
- **Niveau de confiance** global
- **Recommandation** pour investisseur (court terme)"""
        
        return self._call_gemini_with_retry(prompt)

    def _analyze_technical_indicators(self, series_indicators):
        """Analyse les indicateurs techniques"""
        mm_decision = series_indicators.get('mm_decision', 'N/A')
        bollinger_decision = series_indicators.get('bollinger_decision', 'N/A')
        macd_decision = series_indicators.get('macd_decision', 'N/A')
        rsi_decision = series_indicators.get('rsi_decision', 'N/A')
        stochastic_decision = series_indicators.get('stochastic_decision', 'N/A')
        
        indicators_summary = f"""Moyennes Mobiles: {mm_decision}
Bandes de Bollinger: {bollinger_decision}
MACD: {macd_decision}
RSI: {rsi_decision}
Stochastique: {stochastic_decision}"""
        
        prompt = f"""Analyse ces indicateurs techniques (jour récent).

Pour chaque indicateur, analyse concise avec signal clair.

Indicateurs:
{indicators_summary}"""
        
        return self._call_gemini_with_retry(prompt)

    def _summarize_fundamental_analysis(self, summaries):
        """Synthétise les analyses fondamentales"""
        if not summaries or summaries == "Aucune analyse fondamentale disponible.":
            return summaries
        
        summary_preview = summaries[:3000] + "..." if len(summaries) > 3000 else summaries
        
        prompt = f"""Synthétise ces analyses fondamentales en 3-4 points clés.

Concentre-toi sur :
- Chiffre d'affaires
- Résultat net
- Dividendes
- Perspectives

Analyses:
{summary_preview}"""
        
        return self._call_gemini_with_retry(prompt)

    def _add_predictions_table(self, doc, df_predictions):
        """Ajoute un tableau des prédictions dans le document"""
        if df_predictions is None or df_predictions.empty:
            return
        
        # Ajouter un sous-titre
        doc.add_heading('Tableau des Prédictions Détaillées', level=3)
        
        # Créer le tableau (21 lignes: header + 20 prédictions)
        table = doc.add_table(rows=1, cols=5)
        table.style = 'Light Grid Accent 1'
        
        # Header
        hdr_cells = table.rows[0].cells
        hdr_cells[0].text = 'Jour'
        hdr_cells[1].text = 'Date'
        hdr_cells[2].text = 'Prix Prédit (F CFA)'
        hdr_cells[3].text = 'Fourchette (F CFA)'
        hdr_cells[4].text = 'Confiance'
        
        # Rendre le header en gras
        for cell in hdr_cells:
            for paragraph in cell.paragraphs:
                for run in paragraph.runs:
                    run.bold = True
        
        # Ajouter les 20 prédictions
        for idx, row in df_predictions.iterrows():
            row_cells = table.add_row().cells
            row_cells[0].text = f'J+{idx + 1}'
            row_cells[1].text = row['prediction_date'].strftime('%d/%m/%Y')
            row_cells[2].text = f"{row['predicted_price']:.2f}"
            row_cells[3].text = f"{row['lower_bound']:.0f} - {row['upper_bound']:.0f}"
            row_cells[4].text = row['confidence_level']
        
        doc.add_paragraph()  # Espace après le tableau

    def _create_main_report(self, company_analyses):
        """Génère le rapport Word complet"""
        logging.info("📝 Création du rapport de synthèse...")
        
        doc = Document()
        
        # Titre
        title = doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Métadonnées
        meta = doc.add_paragraph()
        meta.add_run(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}\n").bold = True
        meta.add_run(f"Propulsé par {GEMINI_MODEL} (API {GEMINI_API_VERSION})\n")
        meta.add_run(f"Analyse sur 100 jours | Prédictions 20 jours ouvrables (Lun-Ven)\n")
        meta.add_run(f"Base de données : Supabase (PostgreSQL) | Version : 9.0")
        meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        doc.add_paragraph()
        
        # Contenu pour chaque société
        for symbol, analyses in sorted(company_analyses.items()):
            nom_societe = analyses.get('nom_societe', symbol)
            
            heading = doc.add_heading(f'{nom_societe} ({symbol})', level=1)
            heading.runs[0].font.color.rgb = RGBColor(0, 51, 102)
            
            # 1. Évolution du Cours
            doc.add_heading('1. Évolution du Cours (100 derniers jours)', level=2)
            doc.add_paragraph(analyses.get('price_analysis', 'Analyse non disponible.'))
            
            # 2. Prédictions
            if 'predictions_analysis' in analyses:
                doc.add_heading('2. Prédictions (20 prochains jours ouvrables)', level=2)
                doc.add_paragraph(analyses['predictions_analysis'])
                
                # Ajouter le tableau des prédictions
                if 'predictions_table' in analyses:
                    self._add_predictions_table(doc, analyses['predictions_table'])
            
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
        """Génère tous les rapports"""
        logging.info("="*80)
        logging.info("📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V9.0 - GESTION INTELLIGENTE)")
        logging.info("="*80)

        # Stats initiales
        stats = self.api_manager.get_statistics()
        logging.info(f"📊 Clés disponibles: {stats['available']}/{stats['total']}")
        logging.info(f"📊 Clés déjà épuisées: {stats['exhausted']}")
        
        if stats['available'] == 0:
            logging.error("❌ Aucune clé API disponible - Génération impossible")
            return
        
        all_data = self._get_all_data_from_db()
        company_analyses = {}

        for symbol, data in all_data.items():
            logging.info(f"--- Génération: {symbol} ---")
            
            current_price = data['price_data']['price'].iloc[-1] if not data['price_data'].empty else 0
            
            company_analyses[symbol] = {
                'nom_societe': data['nom_societe'],
                'price_analysis': self._analyze_price_evolution(data['price_data']),
                'technical_analysis': self._analyze_technical_indicators(data['indicator_data']),
                'fundamental_summary': self._summarize_fundamental_analysis(data['fundamental_summaries'])
            }
            
            # Analyse détaillée des 20 prédictions
            df_predictions = self._get_predictions_from_db(symbol)
            if df_predictions is not None and not df_predictions.empty:
                company_analyses[symbol]['predictions_analysis'] = self._analyze_predictions_detailed(
                    df_predictions, 
                    current_price
                )
                company_analyses[symbol]['predictions_table'] = df_predictions

        self._create_main_report(company_analyses)
        
        # Stats finales
        final_stats = self.api_manager.get_statistics()
        logging.info("\n✅ Génération terminée")
        logging.info(f"📊 Clés utilisées: {final_stats['used_by_module']}")
        logging.info(f"📊 Clés épuisées: {final_stats['exhausted']}")
        logging.info(f"📊 Clés restantes: {final_stats['available']}")

if __name__ == "__main__":
    db_conn = None
    try:
        if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
            logging.error("❌ Secrets DB manquants")
        else:
            db_conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, 
                host=DB_HOST, port=DB_PORT
            )
            report_generator = ComprehensiveReportGenerator(db_conn)
            report_generator.generate_all_reports([])
    except Exception as e:
        logging.error(f"❌ Erreur: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close()
