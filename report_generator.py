# ==============================================================================
# MODULE: REPORT GENERATOR V21.0 - GEMINI 1.5 FLASH (API V1 + 11 CLÉS)
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

# ✅ CONFIGURATION GEMINI (API V1 STABLE)
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")


class BRVMReportGenerator:
    def __init__(self):
        self.db_conn = None
        self.api_manager = APIKeyManager('report_generator')
        
        # Connexion DB
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
        """Récupération optimisée des données"""
        logging.info("📂 Récupération des données (30 derniers jours)...")
        
        date_limite = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        query = f"""
        WITH recent_data AS (
            SELECT 
                company_id,
                id as historical_data_id,
                trade_date,
                price,
                ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
            FROM historical_data
            WHERE trade_date >= '{date_limite}'
        ),
        latest_per_company AS (
            SELECT * FROM recent_data WHERE rn = 1
        )
        SELECT
            c.symbol, 
            c.name as company_name,
            lpc.trade_date, 
            lpc.price,
            ta.mm_decision, 
            ta.bollinger_decision, 
            ta.macd_decision,
            ta.rsi_decision, 
            ta.stochastic_decision,
            (
                SELECT STRING_AGG(fa.analysis_summary, E'\\n---\\n' ORDER BY fa.report_date DESC) 
                FROM fundamental_analysis fa 
                WHERE fa.company_id = c.id
                LIMIT 5
            ) as fundamental_summaries
        FROM companies c
        LEFT JOIN latest_per_company lpc ON c.id = lpc.company_id
        LEFT JOIN technical_analysis ta ON lpc.historical_data_id = ta.historical_data_id
        ORDER BY c.symbol;
        """
        
        try:
            df = pd.read_sql(query, self.db_conn)
            
            if df.empty:
                logging.warning("⚠️  Aucune donnée récente trouvée")
                return pd.DataFrame()
            
            logging.info(f"   ✅ {len(df)} société(s) récupérée(s)")
            return df
            
        except psycopg2.errors.QueryCanceled:
            logging.error("❌ Timeout SQL")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"❌ Erreur requête SQL: {e}")
            return pd.DataFrame()

    def _get_predictions_from_db(self):
        """Récupération des prédictions"""
        logging.info("🔮 Récupération des prédictions...")
        
        query = """
        WITH latest_predictions AS (
            SELECT 
                company_id,
                prediction_date,
                predicted_price,
                ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY prediction_date DESC) as rn
            FROM predictions
            WHERE prediction_date >= CURRENT_DATE
        )
        SELECT 
            c.symbol,
            lp.prediction_date,
            lp.predicted_price
        FROM companies c
        LEFT JOIN latest_predictions lp ON c.id = lp.company_id AND lp.rn <= 20
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
        """Génération analyse IA avec Gemini (avec limite de tentatives)"""
        
        if attempt > 1:
            logging.info(f"    🔄 {symbol}: Tentative {attempt}/{max_attempts}")
        
        # Obtenir la clé API (avec rotation automatique)
        api_key = self.api_manager.get_api_key()
        if not api_key:
            logging.warning(f"    ⚠️  Aucune clé Gemini disponible pour {symbol}")
            return self._generate_fallback_analysis(symbol, data_dict)
        
        # Construire contexte
        context_parts = [f"Société: {symbol}"]
        
        if data_dict.get('price'):
            context_parts.append(f"Prix actuel: {data_dict['price']:.0f} FCFA")
        
        if data_dict.get('technical_signals'):
            context_parts.append(f"Signaux techniques: {data_dict['technical_signals']}")
        
        if data_dict.get('fundamental_summary'):
            context_parts.append(f"Analyse fondamentale:\n{data_dict['fundamental_summary'][:500]}")
        
        if data_dict.get('predictions'):
            pred_text = ", ".join([f"{p['date']}: {p['price']:.0f}" for p in data_dict['predictions'][:5]])
            context_parts.append(f"Prédictions: {pred_text}")
        
        context = "\n\n".join(context_parts)
        
        prompt = f"""Analyse cette société BRVM et fournis UNE recommandation claire.

{context}

Fournis:
1. **Recommandation**: ACHAT, VENTE ou CONSERVER (1 mot)
2. **Niveau de confiance**: Élevé, Moyen ou Faible
3. **Justification**: 2-3 phrases concises
4. **Niveau de risque**: Faible, Moyen ou Élevé

Sois direct et factuel."""
        
        # ✅ API GEMINI V1 (STABLE) - PAS V1BETA
        api_url = f"https://generativelanguage.googleapis.com/v1/models/{GEMINI_MODEL}:generateContent?key={api_key}"
        
        request_body = {
            "contents": [{
                "parts": [{"text": prompt}]
            }]
        }
        
        try:
            response = requests.post(api_url, json=request_body, timeout=30)
            
            # Enregistrer la requête
            self.api_manager.record_request()
            
            if response.status_code == 200:
                data = response.json()
                if 'candidates' in data and len(data['candidates']) > 0:
                    candidate = data['candidates'][0]
                    if 'content' in candidate and 'parts' in candidate['content']:
                        text = candidate['content']['parts'][0]['text']
                        logging.info(f"    ✅ {symbol}: Analyse générée")
                        return text
                else:
                    logging.warning(f"    ⚠️  Réponse vide pour {symbol}")
                    return self._generate_fallback_analysis(symbol, data_dict)
            
            elif response.status_code == 429:
                # Rate limit - gérer et réessayer AVEC LIMITE
                logging.warning(f"    ⚠️  Rate limit pour {symbol} (tentative {attempt}/{max_attempts})")
                
                # Essayer de changer de clé
                can_retry = self.api_manager.handle_rate_limit_response()
                
                # Réessayer SEULEMENT si < max_attempts ET qu'il y a une clé disponible
                if attempt < max_attempts and can_retry:
                    time.sleep(2)  # Petite pause
                    return self._generate_ia_analysis(symbol, data_dict, attempt + 1, max_attempts)
                else:
                    logging.error(f"    ❌ {symbol}: Échec après {attempt} tentatives - FALLBACK")
                    return self._generate_fallback_analysis(symbol, data_dict)
            
            else:
                logging.error(f"    ❌ Erreur {response.status_code} pour {symbol}: {response.text[:200]}")
                return self._generate_fallback_analysis(symbol, data_dict)
                
        except requests.exceptions.Timeout:
            logging.error(f"    ⏱️  Timeout pour {symbol}")
            return self._generate_fallback_analysis(symbol, data_dict)
        except Exception as e:
            logging.error(f"    ❌ Exception pour {symbol}: {str(e)}")
            return self._generate_fallback_analysis(symbol, data_dict)

    def _generate_fallback_analysis(self, symbol, data_dict):
        """Analyse de secours si API échoue"""
        analysis = f"**Analyse de {symbol}**\n\n"
        
        if data_dict.get('price'):
            analysis += f"Prix actuel: {data_dict['price']:.0f} FCFA\n\n"
        
        if data_dict.get('technical_signals'):
            signals = data_dict['technical_signals']
            buy_count = signals.count('Achat')
            sell_count = signals.count('Vente')
            
            if buy_count > sell_count:
                analysis += "**Recommandation**: ACHAT\n"
                analysis += "Signaux techniques majoritairement positifs.\n\n"
            elif sell_count > buy_count:
                analysis += "**Recommandation**: VENTE\n"
                analysis += "Signaux techniques majoritairement négatifs.\n\n"
            else:
                analysis += "**Recommandation**: CONSERVER\n"
                analysis += "Signaux techniques mixtes.\n\n"
        else:
            analysis += "**Recommandation**: CONSERVER\n"
            analysis += "Données insuffisantes pour une recommandation.\n\n"
        
        analysis += "**Niveau de confiance**: Moyen\n"
        analysis += "**Niveau de risque**: Moyen\n"
        
        return analysis

    def _create_word_document(self, all_analyses):
        """Création du document Word"""
        logging.info("📄 Création du document Word...")
        
        doc = Document()
        
        # En-tête
        title = doc.add_heading('Rapport d\'Analyse BRVM', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        subtitle = doc.add_paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}")
        subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        doc.add_paragraph()
        
        # Synthèse
        doc.add_heading('Synthèse Générale', level=1)
        doc.add_paragraph(f"Nombre de sociétés analysées: {len(all_analyses)}")
        doc.add_paragraph()
        
        # Analyses détaillées
        doc.add_heading('Analyses Détaillées', level=1)
        
        for symbol, analysis in sorted(all_analyses.items()):
            doc.add_heading(f"{symbol}", level=2)
            doc.add_paragraph(analysis)
            doc.add_paragraph()
        
        # Sauvegarde
        filename = f"Rapport_Synthese_Investissement_BRVM_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(filename)
        
        logging.info(f"   ✅ Document créé: {filename}")
        return filename

    def generate_all_reports(self, new_fundamental_analyses):
        """Génération du rapport complet"""
        logging.info("="*80)
        logging.info("📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V21.0 - Gemini 1.5 Flash)")
        logging.info(f"🤖 Modèle: {GEMINI_MODEL}")
        logging.info("="*80)
        
        stats = self.api_manager.get_statistics()
        logging.info(f"📊 Clés Gemini: {stats['available']}/{stats['total']} disponible(s)")
        
        # Récupération données
        df = self._get_all_data_from_db()
        
        if df.empty:
            logging.error("❌ Aucune donnée disponible - rapport impossible")
            return
        
        predictions_df = self._get_predictions_from_db()
        
        # Génération analyses IA
        logging.info(f"🤖 Génération de {len(df)} analyse(s) IA avec limite 3 tentatives...")
        
        all_analyses = {}
        
        for idx, row in df.iterrows():
            symbol = row['symbol']
            
            # Préparer contexte
            data_dict = {
                'price': row.get('price'),
                'technical_signals': None,
                'fundamental_summary': row.get('fundamental_summaries'),
                'predictions': []
            }
            
            # Signaux techniques
            if row.get('mm_decision'):
                signals = [
                    row.get('mm_decision'),
                    row.get('bollinger_decision'),
                    row.get('macd_decision'),
                    row.get('rsi_decision'),
                    row.get('stochastic_decision')
                ]
                data_dict['technical_signals'] = ", ".join([s for s in signals if s])
            
            # Prédictions
            symbol_predictions = predictions_df[predictions_df['symbol'] == symbol]
            if not symbol_predictions.empty:
                data_dict['predictions'] = [
                    {'date': row['prediction_date'], 'price': row['predicted_price']}
                    for _, row in symbol_predictions.iterrows()
                ]
            
            # Génération analyse IA
            analysis = self._generate_ia_analysis(symbol, data_dict)
            all_analyses[symbol] = analysis
        
        # Création document
        filename = self._create_word_document(all_analyses)
        
        final_stats = self.api_manager.get_statistics()
        logging.info(f"\n✅ Rapport généré: {filename}")
        logging.info(f"📊 Requêtes effectuées: {final_stats['used_by_module']}")

    def __del__(self):
        """Fermeture connexion DB"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()


if __name__ == "__main__":
    try:
        report_generator = BRVMReportGenerator()
        report_generator.generate_all_reports([])
    except Exception as e:
        logging.critical(f"❌ Erreur: {e}", exc_info=True)
