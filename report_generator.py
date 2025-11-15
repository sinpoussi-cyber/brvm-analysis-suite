# ==============================================================================
# MODULE: REPORT GENERATOR V24.0 - OPENAI GPT-4o
# ==============================================================================

import os
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from docx import Document
from docx.shared import Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
import openai  # Import de la bibliothèque OpenAI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION OPENAI GPT-4o (NOUVEAU)
OPENAI_MODEL = "gpt-4o"

class BRVMReportGenerator:
    def __init__(self):
        self.db_conn = None
        
        try:
            self.db_conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                host=DB_HOST, port=DB_PORT, connect_timeout=10
            )
            logging.info("✅ Connexion DB établie")
        except Exception as e:
            logging.error(f"❌ Erreur connexion DB: {e}")
            raise

        # Initialisation du client OpenAI
        try:
            self.openai_client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            logging.info("✅ Client OpenAI initialisé pour la génération de rapports.")
        except Exception as e:
            self.openai_client = None
            logging.error(f"❌ Erreur initialisation client OpenAI pour rapports: {e}")

    def _get_all_data_from_db(self):
        logging.info("📂 Récupération des données (30 derniers jours)...")
        date_limite = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        query = f"""
        WITH recent_data AS (
            SELECT company_id, id as historical_data_id, trade_date, price,
                   ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
            FROM historical_data WHERE trade_date >= '{date_limite}'
        ), latest_per_company AS (SELECT * FROM recent_data WHERE rn = 1)
        SELECT c.symbol, c.name as company_name, lpc.trade_date, lpc.price,
               ta.mm_decision, ta.bollinger_decision, ta.macd_decision,
               ta.rsi_decision, ta.stochastic_decision,
               (SELECT STRING_AGG(fa.analysis_summary, E'\\n---\\n' ORDER BY fa.report_date DESC LIMIT 3) 
                FROM fundamental_analysis fa WHERE fa.company_id = c.id) as fundamental_summaries
        FROM companies c
        LEFT JOIN latest_per_company lpc ON c.id = lpc.company_id
        LEFT JOIN technical_analysis ta ON lpc.historical_data_id = ta.historical_data_id
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
            SELECT company_id, prediction_date, predicted_price,
                   ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY prediction_date ASC) as rn
            FROM predictions WHERE prediction_date >= CURRENT_DATE
        ) lp ON c.id = lp.company_id AND lp.rn <= 5
        ORDER BY c.symbol, lp.prediction_date;
        """
        try:
            df = pd.read_sql(query, self.db_conn)
            logging.info(f"   ✅ {len(df)} prédiction(s)")
            return df
        except Exception as e:
            logging.error(f"❌ Erreur prédictions: {e}")
            return pd.DataFrame()

    def _generate_ia_analysis(self, symbol, data_dict):
        if not self.openai_client:
            logging.warning(f"    ⚠️  Client OpenAI non disponible pour {symbol}, utilisation du fallback.")
            return self._generate_fallback_analysis(symbol, data_dict)

        context_parts = [f"**Société : {symbol} ({data_dict.get('company_name', '')})**"]
        if data_dict.get('price'): context_parts.append(f"Dernier cours : {data_dict['price']:.0f} FCFA")
        if data_dict.get('technical_signals'): context_parts.append(f"Signaux Techniques Clés : {data_dict['technical_signals']}")
        if data_dict.get('predictions'):
            pred_text = ", ".join([f"J+{i+1}: {p['price']:.0f}" for i, p in enumerate(data_dict['predictions'][:3])])
            context_parts.append(f"Prédictions (3 prochains jours) : {pred_text}")
        if data_dict.get('fundamental_summary'): context_parts.append(f"**Synthèse Fondamentale Récente :**\n{data_dict['fundamental_summary'][:1000]}")
        
        context = "\n".join(context_parts)
        
        prompt = f"""Tu es un conseiller en investissement pour la bourse BRVM. En te basant sur les données suivantes, fournis une recommandation d'investissement claire et concise.

{context}

Ta réponse doit obligatoirement suivre ce format :
1.  **Recommandation :** Un seul mot (ACHAT, VENTE, ou CONSERVER).
2.  **Niveau de Confiance :** Un seul mot (Élevé, Moyen, ou Faible).
3.  **Justification (2-3 phrases) :** Explique ton raisonnement en te basant sur la convergence (ou divergence) des signaux techniques, fondamentaux et des prédictions.
4.  **Niveau de Risque :** Un seul mot (Faible, Moyen, ou Élevé).
"""
        try:
            response = self.openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "Tu es un conseiller expert en investissement sur la BRVM."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_tokens=512
            )
            analysis_text = response.choices[0].message.content
            logging.info(f"    ✅ {symbol}: Analyse de synthèse générée par OpenAI")
            return analysis_text
        except openai.APIError as e:
            logging.error(f"    ❌ Erreur API OpenAI pour {symbol}: {e}")
            return self._generate_fallback_analysis(symbol, data_dict)
        except Exception as e:
            logging.error(f"    ❌ Exception pour {symbol}: {str(e)}")
            return self._generate_fallback_analysis(symbol, data_dict)

    def _generate_fallback_analysis(self, symbol, data_dict):
        buy_count = str(data_dict.get('technical_signals', '')).count('Achat')
        sell_count = str(data_dict.get('technical_signals', '')).count('Vente')
        
        reco = "CONSERVER"
        if buy_count > sell_count + 1: reco = "ACHAT"
        if sell_count > buy_count + 1: reco = "VENTE"
            
        return f"""**Recommandation :** {reco}
**Niveau de Confiance :** Faible
**Justification (2-3 phrases) :** Analyse IA indisponible. La recommandation est basée sur une simple majorité de signaux techniques. Les aspects fondamentaux et prédictifs ne sont pas pris en compte.
**Niveau de Risque :** Élevé
"""

    def _create_word_document(self, all_analyses):
        logging.info("📄 Création du document Word...")
        doc = Document()
        doc.add_heading('Rapport d\'Analyse Stratégique BRVM', 0).alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}", style='Caption').alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph()
        
        doc.add_heading('Synthèse des Recommandations', level=1)
        
        for symbol, analysis in sorted(all_analyses.items()):
            doc.add_heading(f"{analysis.get('company_name', symbol)} ({symbol})", level=2)
            doc.add_paragraph(analysis.get('ia_analysis', 'Analyse non disponible.'))
            doc.add_paragraph()
        
        filename = f"Rapport_Synthese_Investissement_BRVM_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(filename)
        logging.info(f"   ✅ Document créé: {filename}")
        return filename

    def generate_all_reports(self, new_fundamental_analyses):
        logging.info("="*80)
        logging.info(f"📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V24.0 - OpenAI {OPENAI_MODEL})")
        logging.info("="*80)
        
        if not self.openai_client:
            logging.error("❌ Génération de rapports annulée: clé API OpenAI non configurée.")
            return

        df = self._get_all_data_from_db()
        if df.empty:
            logging.error("❌ Aucune donnée disponible - rapport impossible")
            return
        
        predictions_df = self._get_predictions_from_db()
        
        logging.info(f"🤖 Génération de {len(df)} synthèse(s) IA...")
        all_analyses = {}
        for idx, row in df.iterrows():
            symbol = row['symbol']
            signals = [row.get(col) for col in ['mm_decision', 'bollinger_decision', 'macd_decision', 'rsi_decision', 'stochastic_decision']]
            
            data_dict = {
                'company_name': row.get('company_name'),
                'price': row.get('price'),
                'technical_signals': ", ".join([s for s in signals if s]),
                'fundamental_summary': row.get('fundamental_summaries'),
                'predictions': [{'date': r['prediction_date'], 'price': r['predicted_price']} for _, r in predictions_df[predictions_df['symbol'] == symbol].iterrows()]
            }
            
            all_analyses[symbol] = {
                'ia_analysis': self._generate_ia_analysis(symbol, data_dict),
                'company_name': data_dict['company_name']
            }
        
        self._create_word_document(all_analyses)
        logging.info(f"\n✅ Rapports terminés.")

    def __del__(self):
        if self.db_conn:
            self.db_conn.close()

if __name__ == "__main__":
    try:
        report_generator = BRVMReportGenerator()
        report_generator.generate_all_reports([])
    except Exception as e:
        logging.critical(f"❌ Erreur critique: {e}", exc_info=True)
