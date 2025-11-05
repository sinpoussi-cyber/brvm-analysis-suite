# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR V9.2 - VERSION FINALE CORRIGÉE
# ==============================================================================
# CORRECTIONS CRITIQUES:
# - Modèle changé: gemini-1.5-flash (au lieu de gemini-1.5-flash-latest)
# - Version API: v1beta (confirmé fonctionnel)
# - Meilleure gestion des erreurs 404 avec diagnostic
# ==============================================================================

import psycopg2
import pandas as pd
import os
import time
import logging
from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
import requests
from datetime import datetime

from api_key_manager import APIKeyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION GEMINI CORRIGÉE
GEMINI_MODEL = "gemini-1.5-flash"  # ⚠️ SANS "-latest"
GEMINI_API_VERSION = "v1"  # ⚠️ v1 pour gemini-1.5-flash (v1beta ne marche pas)

class ComprehensiveReportGenerator:
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.api_manager = APIKeyManager('report_generator')
        self.current_api_key = None

    def _get_next_api_key(self):
        key_info = self.api_manager.get_next_key()
        if key_info:
            self.current_api_key = key_info
            return key_info['key']
        return None

    def _call_gemini_with_retry(self, prompt):
        """
        Appel API Gemini avec retry - VERSION CORRIGÉE
        """
        available_keys = self.api_manager.get_available_keys()
        
        if not available_keys:
            return "Analyse IA non disponible (toutes les clés épuisées)."
        
        max_attempts = len(available_keys)
        attempts = 0
        
        while attempts < max_attempts:
            api_key = self._get_next_api_key()
            
            if not api_key:
                return "Erreur d'analyse : Toutes les clés API ont échoué."
            
            key_num = self.current_api_key['number']
            self.api_manager.handle_rate_limit()
            
            # ✅ URL CORRIGÉE
            api_url = f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models/{GEMINI_MODEL}:generateContent"
            
            headers = {
                "Content-Type": "application/json",
                "x-goog-api-key": api_key
            }
            
            request_body = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.7,
                    "topK": 40,
                    "topP": 0.95,
                    "maxOutputTokens": 2048,
                }
            }
            
            try:
                response = requests.post(
                    api_url, 
                    headers=headers, 
                    json=request_body, 
                    timeout=90
                )

                if response.status_code == 200:
                    try:
                        response_json = response.json()
                        
                        if not response_json:
                            logging.error(f"    ❌ Réponse JSON vide (clé #{key_num})")
                            self.api_manager.move_to_next_key()
                            attempts += 1
                            continue
                        
                        if 'candidates' not in response_json:
                            logging.error(f"    ❌ Pas de 'candidates' dans réponse (clé #{key_num})")
                            self.api_manager.move_to_next_key()
                            attempts += 1
                            continue
                        
                        candidates = response_json['candidates']
                        if not candidates or len(candidates) == 0:
                            logging.error(f"    ❌ Liste candidates vide (clé #{key_num})")
                            self.api_manager.move_to_next_key()
                            attempts += 1
                            continue
                        
                        candidate = candidates[0]
                        if 'content' not in candidate:
                            logging.error(f"    ❌ Pas de 'content' dans candidate (clé #{key_num})")
                            self.api_manager.move_to_next_key()
                            attempts += 1
                            continue
                        
                        content = candidate['content']
                        if 'parts' not in content or not content['parts']:
                            logging.error(f"    ❌ Pas de 'parts' dans content (clé #{key_num})")
                            self.api_manager.move_to_next_key()
                            attempts += 1
                            continue
                        
                        text = content['parts'][0].get('text', '')
                        
                        if not text or len(text.strip()) == 0:
                            logging.error(f"    ❌ Texte vide dans réponse (clé #{key_num})")
                            self.api_manager.move_to_next_key()
                            attempts += 1
                            continue
                        
                        return text
                        
                    except (KeyError, IndexError, TypeError) as e:
                        logging.error(f"    ❌ Erreur parsing JSON (clé #{key_num}): {e}")
                        self.api_manager.move_to_next_key()
                        attempts += 1
                        continue
                
                elif response.status_code == 429:
                    logging.warning(f"    ⚠️  Quota épuisé (clé #{key_num})")
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                elif response.status_code == 404:
                    # ✅ MEILLEURE GESTION DU 404
                    try:
                        error_detail = response.json()
                        error_msg = error_detail.get('error', {}).get('message', 'Endpoint introuvable')
                        logging.error(f"    ❌ 404 - {error_msg}")
                        logging.error(f"    URL: {api_url}")
                        logging.error(f"    Modèle: {GEMINI_MODEL}")
                    except:
                        logging.error(f"    ❌ 404 - Endpoint ou modèle introuvable")
                    
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                elif response.status_code == 403:
                    logging.error(f"    ❌ 403 (clé #{key_num})")
                    self.api_manager.mark_key_exhausted(key_num)
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
                
                else:
                    logging.error(f"    ❌ Erreur {response.status_code} (clé #{key_num})")
                    self.api_manager.move_to_next_key()
                    attempts += 1
                    continue
            
            except requests.exceptions.Timeout:
                logging.error(f"    ⏱️  Timeout (clé #{key_num})")
                self.api_manager.move_to_next_key()
                attempts += 1
                continue
                
            except requests.exceptions.ConnectionError as e:
                logging.error(f"    ❌ Erreur connexion (clé #{key_num}): {e}")
                self.api_manager.move_to_next_key()
                attempts += 1
                continue
                
            except Exception as e:
                logging.error(f"    ❌ Exception (clé #{key_num}): {e}")
                self.api_manager.move_to_next_key()
                attempts += 1
                continue
        
        return "Erreur d'analyse : Toutes les tentatives ont échoué."

    def _get_all_data_from_db(self):
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
        try:
            query = """
            SELECT prediction_date, predicted_price, lower_bound, upper_bound, confidence_level
            FROM predictions p
            JOIN companies c ON p.company_id = c.id
            WHERE c.symbol = %s
            ORDER BY p.prediction_date
            """
            df = pd.read_sql(query, self.db_conn, params=(symbol,))
            return df if not df.empty else None
        except Exception as e:
            logging.error(f"❌ Erreur récupération prédictions {symbol}: {e}")
            return None

    def _analyze_price_evolution(self, df_prices):
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
        if df_predictions is None or df_predictions.empty:
            return "Aucune prédiction disponible."
        
        predictions_by_week = []
        
        week1 = df_predictions.iloc[0:5]
        w1_avg = week1['predicted_price'].mean()
        predictions_by_week.append(f"Semaine 1 (J+1 à J+5): Moyenne {w1_avg:.0f} F CFA")
        
        week2 = df_predictions.iloc[5:10]
        w2_avg = week2['predicted_price'].mean()
        predictions_by_week.append(f"Semaine 2 (J+6 à J+10): Moyenne {w2_avg:.0f} F CFA")
        
        week3_4 = df_predictions.iloc[10:20]
        w3_4_avg = week3_4['predicted_price'].mean()
        predictions_by_week.append(f"Semaines 3-4 (J+11 à J+20): Moyenne {w3_4_avg:.0f} F CFA")
        
        predictions_text = "\n".join(predictions_by_week)
        
        j1 = df_predictions.iloc[0]
        j20 = df_predictions.iloc[19]
        
        key_days = f"""J+1: {j1['predicted_price']:.0f} F CFA
J+20: {j20['predicted_price']:.0f} F CFA"""
        
        last_pred = df_predictions.iloc[-1]['predicted_price']
        change_percent = ((last_pred - current_price) / current_price * 100)
        
        prompt = f"""Analyse ces prédictions de prix pour les 20 prochains jours ouvrables.

Prix actuel: {current_price:.2f} F CFA
Variation prévue: {change_percent:.2f}%

RÉSUMÉ PAR PÉRIODES:
{predictions_text}

JOURS CLÉS:
{key_days}

Fournis une analyse concise avec :
- **Tendance prévue** sur les 20 jours
- **Évolution par phases**
- **Fourchettes de prix** : min/max attendus
- **Recommandation** pour investisseur"""
        
        return self._call_gemini_with_retry(prompt)

    def _analyze_technical_indicators(self, series_indicators):
        mm = series_indicators.get('mm_decision', 'N/A')
        bollinger = series_indicators.get('bollinger_decision', 'N/A')
        macd = series_indicators.get('macd_decision', 'N/A')
        rsi = series_indicators.get('rsi_decision', 'N/A')
        stoch = series_indicators.get('stochastic_decision', 'N/A')
        
        indicators_summary = f"""Moyennes Mobiles: {mm}
Bollinger: {bollinger}
MACD: {macd}
RSI: {rsi}
Stochastique: {stoch}"""
        
        prompt = f"""Analyse ces indicateurs techniques.

{indicators_summary}

Fournis une analyse concise avec signal clair."""
        
        return self._call_gemini_with_retry(prompt)

    def _summarize_fundamental_analysis(self, summaries):
        if not summaries or summaries == "Aucune analyse fondamentale disponible.":
            return summaries
        
        summary_preview = summaries[:3000] + "..." if len(summaries) > 3000 else summaries
        
        prompt = f"""Synthétise ces analyses fondamentales en 3-4 points clés.

Concentre-toi sur : CA, Résultat Net, Dividendes, Perspectives

Analyses:
{summary_preview}"""
        
        return self._call_gemini_with_retry(prompt)

    def _add_predictions_table(self, doc, df_predictions):
        if df_predictions is None or df_predictions.empty:
            return
        
        doc.add_heading('Tableau des Prédictions Détaillées', level=3)
        
        table = doc.add_table(rows=1, cols=5)
        table.style = 'Light Grid Accent 1'
        
        hdr_cells = table.rows[0].cells
        hdr_cells[0].text = 'Jour'
        hdr_cells[1].text = 'Date'
        hdr_cells[2].text = 'Prix Prédit (F CFA)'
        hdr_cells[3].text = 'Fourchette (F CFA)'
        hdr_cells[4].text = 'Confiance'
        
        for cell in hdr_cells:
            for paragraph in cell.paragraphs:
                for run in paragraph.runs:
                    run.bold = True
        
        for idx, row in df_predictions.iterrows():
            row_cells = table.add_row().cells
            row_cells[0].text = f'J+{idx + 1}'
            row_cells[1].text = row['prediction_date'].strftime('%d/%m/%Y')
            row_cells[2].text = f"{row['predicted_price']:.2f}"
            row_cells[3].text = f"{row['lower_bound']:.0f} - {row['upper_bound']:.0f}"
            row_cells[4].text = row['confidence_level']
        
        doc.add_paragraph()

    def _create_main_report(self, company_analyses):
        logging.info("📝 Création du rapport de synthèse...")
        
        doc = Document()
        
        title = doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        meta = doc.add_paragraph()
        meta.add_run(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}\n").bold = True
        meta.add_run(f"Propulsé par {GEMINI_MODEL} (API {GEMINI_API_VERSION})\n")
        meta.add_run(f"Analyse sur 100 jours | Prédictions 20 jours ouvrables\n")
        meta.add_run(f"Base de données : Supabase (PostgreSQL) | Version : 9.2")
        meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        doc.add_paragraph()
        
        for symbol, analyses in sorted(company_analyses.items()):
            nom_societe = analyses.get('nom_societe', symbol)
            
            heading = doc.add_heading(f'{nom_societe} ({symbol})', level=1)
            heading.runs[0].font.color.rgb = RGBColor(0, 51, 102)
            
            doc.add_heading('1. Évolution du Cours (100 derniers jours)', level=2)
            doc.add_paragraph(analyses.get('price_analysis', 'Analyse non disponible.'))
            
            if 'predictions_analysis' in analyses:
                doc.add_heading('2. Prédictions (20 prochains jours ouvrables)', level=2)
                doc.add_paragraph(analyses['predictions_analysis'])
                
                if 'predictions_table' in analyses:
                    self._add_predictions_table(doc, analyses['predictions_table'])
            
            doc.add_heading('3. Analyse Technique des Indicateurs', level=2)
            doc.add_paragraph(analyses.get('technical_analysis', 'Analyse technique non disponible.'))
            
            doc.add_heading('4. Synthèse Fondamentale', level=2)
            doc.add_paragraph(analyses.get('fundamental_summary', 'Analyse fondamentale non disponible.'))
            
            doc.add_page_break()

        output_filename = f"Rapport_Synthese_Investissement_BRVM_{time.strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_filename)
        
        logging.info(f"🎉 Rapport généré: {output_filename}")
        return output_filename

    def generate_all_reports(self, new_fundamental_analyses):
        logging.info("="*80)
        logging.info("📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V9.2 - CORRIGÉE)")
        logging.info(f"🤖 Modèle: {GEMINI_MODEL} | API: {GEMINI_API_VERSION}")
        logging.info("="*80)

        stats = self.api_manager.get_statistics()
        logging.info(f"📊 Clés disponibles: {stats['available']}/{stats['total']}")
        
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
            
            df_predictions = self._get_predictions_from_db(symbol)
            if df_predictions is not None and not df_predictions.empty:
                company_analyses[symbol]['predictions_analysis'] = self._analyze_predictions_detailed(
                    df_predictions, current_price
                )
                company_analyses[symbol]['predictions_table'] = df_predictions

        self._create_main_report(company_analyses)
        
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
