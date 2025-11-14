# ==============================================================================
# MODULE: REPORT GENERATOR V21.0 - GEMINI-PRO (SOLUTION STABLE)
# ==============================================================================

import os
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
import requests
import time

from api_key_manager import APIKeyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ SOLUTION: Utilisation du modèle stable et universel gemini-pro
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-pro")


class BRVMReportGenerator:
    def __init__(self):
        self.db_conn = None
        self.api_manager = APIKeyManager('report_generator')
        
        try:
            self.db_conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                host=DB_HOST, port=DB_PORT,
                connect_timeout=10,
                options='-c statement_timeout=45000'
            )
            logging.info("✅ Connexion DB établie")
        except Exception as e:
            logging.error(f"❌ Erreur connexion DB: {e}")
            raise

    def _get_all_data_from_db(self):
        logging.info("📂 Récupération des données (30 derniers jours)...")
        date_limite = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        query = f"""
        SELECT c.symbol, c.name as company_name, lpc.trade_date, lpc.price,
               ta.mm_decision, ta.bollinger_decision, ta.macd_decision,
               ta.rsi_decision, ta.stochastic_decision,
               (SELECT STRING_AGG(fa.analysis_summary, E'\\n---\\n' ORDER BY fa.report_date DESC) 
                FROM fundamental_analysis fa WHERE fa.company_id = c.id LIMIT 5) as fundamental_summaries
        FROM companies c
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
            FROM historical_data WHERE trade_date >= '{date_limite}'
        ) lpc ON c.id = lpc.company_id AND lpc.rn = 1
        LEFT JOIN technical_analysis ta ON lpc.id = ta.historical_data_id
        ORDER BY c.symbol;
        """
        try:
            df = pd.read_sql(query, self.db_conn)
            logging.info(f"   ✅ {len(df)} société(s) récupérée(s)")
            return df
        except Exception as e:
            logging.error(f"❌ Erreur requête SQL: {e}")
            return pd.DataFrame()

    def _get_predictions_from_db(self):
        logging.info("🔮 Récupération des prédictions...")
        query = """
        SELECT c.symbol, lp.prediction_date, lp.predicted_price
        FROM companies c
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY prediction_date ASC) as rn
            FROM predictions WHERE prediction_date >= CURRENT_DATE
        ) lp ON c.id = lp.company_id AND lp.rn <= 20
        ORDER BY c.symbol, lp.prediction_date;
        """
        try:
            df = pd.read_sql(query, self.db_conn)
            logging.info(f"   ✅ {len(df)} prédiction(s)")
            return df
        except Exception as e:
            logging.error(f"❌ Erreur prédictions: {e}")
            return pd.DataFrame()

    def _generate_ia_analysis(self, symbol, data_dict, attempt=1, max_attempts=3):
        if attempt > 1:
            logging.info(f"    🔄 {symbol}: Tentative {attempt}/{max_attempts}")
        
        api_key = self.api_manager.get_api_key()
        if not api_key:
            logging.warning(f"    ⚠️  Aucune clé Gemini disponible pour {symbol}")
            return self._generate_fallback_analysis(symbol, data_dict)
        
        context_parts = [f"Société: {symbol}"]
        if data_dict.get('price'): context_parts.append(f"Prix actuel: {data_dict['price']:.0f} FCFA")
        if data_dict.get('technical_signals'): context_parts.append(f"Signaux techniques: {data_dict['technical_signals']}")
        if data_dict.get('fundamental_summary'): context_parts.append(f"Synthèse fondamentale:\n{data_dict['fundamental_summary'][:500]}")
        if data_dict.get('predictions'):
            pred_text = ", ".join([f"{p['date'].strftime('%d/%m')}: {p['price']:.0f}" for p in data_dict['predictions'][:5]])
            context_parts.append(f"Prédictions (J: PRIX): {pred_text}")
        context = "\n\n".join(context_parts)
        
        prompt = f"""Tu es un analyste financier expert de la BRVM. Analyse les données fournies pour la société et donne une recommandation d'investissement claire et concise.
{context}

Format de réponse obligatoire :
1.  **Recommandation** : ACHAT, VENTE, ou CONSERVER (un seul mot).
2.  **Confiance** : Élevée, Moyenne, ou Faible.
3.  **Justification** : 2-3 phrases expliquant ta décision, basées sur les données.
4.  **Risque** : Faible, Moyen, ou Élevé."""
        
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
        headers = {'Content-Type': 'application/json', 'x-goog-api-key': api_key}
        request_body = {"contents": [{"parts": [{"text": prompt}]}]}
        
        try:
            response = requests.post(api_url, headers=headers, json=request_body, timeout=30)
            self.api_manager.record_request()
            
            if response.status_code == 200:
                data = response.json()
                if 'candidates' in data and data['candidates']:
                    text = data['candidates'][0]['content']['parts'][0]['text']
                    logging.info(f"    ✅ {symbol}: Analyse générée")
                    return text
                logging.warning(f"    ⚠️  Réponse vide pour {symbol}")
                return self._generate_fallback_analysis(symbol, data_dict)
            
            elif response.status_code == 429:
                logging.warning(f"    ⚠️  Rate limit pour {symbol} (tentative {attempt}/{max_attempts})")
                if attempt < max_attempts and self.api_manager.handle_rate_limit_response():
                    time.sleep(2)
                    return self._generate_ia_analysis(symbol, data_dict, attempt + 1, max_attempts)
                else:
                    logging.error(f"    ❌ {symbol}: Échec après {attempt} tentatives.")
                    return self._generate_fallback_analysis(symbol, data_dict)
            else:
                logging.error(f"    ❌ Erreur {response.status_code} pour {symbol}: {response.text[:200]}")
                return self._generate_fallback_analysis(symbol, data_dict)
        except (requests.exceptions.Timeout, Exception) as e:
            logging.error(f"    ❌ Exception pour {symbol}: {str(e)}")
            return self._generate_fallback_analysis(symbol, data_dict)

    def _generate_fallback_analysis(self, symbol, data_dict):
        analysis = f"**Recommandation**: CONSERVER\n**Confiance**: Faible\n**Justification**: L'analyse automatique par IA a échoué. Une évaluation manuelle des données techniques et fondamentales est nécessaire.\n**Risque**: Moyen"
        return analysis

    def _create_word_document(self, all_analyses):
        logging.info("📄 Création du document Word...")
        doc = Document()
        doc.add_heading('Rapport d\'Analyse BRVM', 0).alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}").alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph()
        doc.add_heading('Analyses Détaillées par Société', level=1)
        
        for symbol, analysis in sorted(all_analyses.items()):
            doc.add_heading(symbol, level=2)
            doc.add_paragraph(analysis)
            doc.add_paragraph()
        
        filename = f"Rapport_Synthese_Investissement_BRVM_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(filename)
        logging.info(f"   ✅ Document créé: {filename}")
        return filename

    def generate_all_reports(self, new_fundamental_analyses):
        logging.info("="*80)
        logging.info(f"📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V21.0 - {GEMINI_MODEL})")
        logging.info("="*80)
        
        stats = self.api_manager.get_statistics()
        logging.info(f"📊 Clés Gemini: {stats['available']}/{stats['total']} disponible(s)")
        
        df = self._get_all_data_from_db()
        if df.empty:
            logging.error("❌ Aucune donnée disponible - rapport impossible")
            return
        
        predictions_df = self._get_predictions_from_db()
        
        logging.info(f"🤖 Génération de {len(df)} analyse(s) IA avec {GEMINI_MODEL}...")
        
        all_analyses = {}
        for idx, row in df.iterrows():
            symbol = row['symbol']
            
            data_dict = {
                'price': row.get('price'),
                'fundamental_summary': row.get('fundamental_summaries'),
                'technical_signals': ", ".join([s for s in [row.get('mm_decision'), row.get('bollinger_decision'), row.get('macd_decision'), row.get('rsi_decision'), row.get('stochastic_decision')] if s and s != 'Attendre']),
                'predictions': [{'date': r['prediction_date'], 'price': r['predicted_price']} for _, r in predictions_df[predictions_df['symbol'] == symbol].iterrows()]
            }
            
            analysis = self._generate_ia_analysis(symbol, data_dict)
            all_analyses[symbol] = analysis
        
        filename = self._create_word_document(all_analyses)
        
        final_stats = self.api_manager.get_statistics()
        logging.info(f"\n✅ Rapport généré: {filename}")
        logging.info(f"📊 Requêtes effectuées: {final_stats['used_by_module']}")

    def __del__(self):
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

if __name__ == "__main__":
    try:
        report_generator = BRVMReportGenerator()
        report_generator.generate_all_reports([])
    except Exception as e:
        logging.critical(f"❌ Erreur: {e}", exc_info=True)
