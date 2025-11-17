# ==============================================================================
# MODULE: REPORT GENERATOR V26.0 - RAPPORT PROFESSIONNEL DÉTAILLÉ
# ==============================================================================

import os
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import requests
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION MISTRAL AI
MISTRAL_API_KEY = os.environ.get('MISTRAL_API_KEY')
MISTRAL_MODEL = "mistral-large-latest"
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"


class BRVMReportGenerator:
    def __init__(self):
        self.db_conn = None
        self.request_count = 0
        
        # Connexion DB
        try:
            self.db_conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                host=DB_HOST, port=DB_PORT,
                connect_timeout=10,
                options='-c statement_timeout=60000'
            )
            logging.info("✅ Connexion DB établie")
        except Exception as e:
            logging.error(f"❌ Erreur connexion DB: {e}")
            raise

    def _get_historical_data_100days(self, company_id):
        """Récupère les 100 derniers jours de données historiques"""
        query = f"""
        SELECT trade_date, price, volume
        FROM historical_data
        WHERE company_id = {company_id}
        ORDER BY trade_date DESC
        LIMIT 100;
        """
        
        try:
            df = pd.read_sql(query, self.db_conn)
            if not df.empty:
                df = df.sort_values('trade_date')
            return df
        except Exception as e:
            logging.error(f"❌ Erreur récupération historique: {e}")
            return pd.DataFrame()

    def _get_all_data_from_db(self):
        """Récupération optimisée des données avec company_id"""
        logging.info("📂 Récupération des données (30 derniers jours)...")
        
        date_limite = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        query = f"""
        WITH recent_data AS (
            SELECT 
                company_id,
                id as historical_data_id,
                trade_date,
                price,
                volume,
                ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
            FROM historical_data
            WHERE trade_date >= '{date_limite}'
        ),
        latest_per_company AS (
            SELECT * FROM recent_data WHERE rn = 1
        )
        SELECT
            c.id as company_id,
            c.symbol, 
            c.name as company_name,
            lpc.trade_date, 
            lpc.price,
            lpc.volume,
            ta.mm_20,
            ta.mm_50,
            ta.mm_decision,
            ta.bollinger_upper,
            ta.bollinger_lower,
            ta.bollinger_decision,
            ta.macd_value,
            ta.macd_signal,
            ta.macd_decision,
            ta.rsi_value,
            ta.rsi_decision,
            ta.stochastic_k,
            ta.stochastic_d,
            ta.stochastic_decision,
            (
                SELECT STRING_AGG(
                    fa.report_title || '|||' || 
                    fa.report_date || '|||' || 
                    fa.analysis_summary, 
                    '###REPORT###' 
                    ORDER BY fa.report_date DESC
                ) 
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

    def _generate_professional_analysis(self, symbol, data_dict, attempt=1, max_attempts=3):
        """Génération analyse professionnelle détaillée avec Mistral AI"""
        
        if attempt > 1:
            logging.info(f"    🔄 {symbol}: Tentative {attempt}/{max_attempts}")
        
        if not MISTRAL_API_KEY:
            logging.warning(f"    ⚠️  Aucune clé Mistral disponible pour {symbol}")
            return self._generate_fallback_analysis(symbol, data_dict)
        
        # Construire le contexte détaillé
        prompt = f"""Tu es un analyste financier professionnel. Analyse l'action {symbol} et génère un rapport structuré en 4 parties.

📊 DONNÉES DISPONIBLES:

**Évolution du cours (100 derniers jours):**
{data_dict.get('historical_summary', 'Données non disponibles')}

**Indicateurs techniques:**
- Moyennes Mobiles: MM20={data_dict.get('mm_20', 'N/A')}, MM50={data_dict.get('mm_50', 'N/A')}, Décision={data_dict.get('mm_decision', 'N/A')}
- Bandes de Bollinger: Borne supérieure={data_dict.get('bollinger_upper', 'N/A')}, Borne inférieure={data_dict.get('bollinger_lower', 'N/A')}, Prix actuel={data_dict.get('price', 'N/A')}, Décision={data_dict.get('bollinger_decision', 'N/A')}
- MACD: Valeur={data_dict.get('macd_value', 'N/A')}, Signal={data_dict.get('macd_signal', 'N/A')}, Décision={data_dict.get('macd_decision', 'N/A')}
- RSI: Valeur={data_dict.get('rsi_value', 'N/A')}, Décision={data_dict.get('rsi_decision', 'N/A')}
- Stochastique: %K={data_dict.get('stochastic_k', 'N/A')}, %D={data_dict.get('stochastic_d', 'N/A')}, Décision={data_dict.get('stochastic_decision', 'N/A')}

**Analyses fondamentales (rapports financiers):**
{data_dict.get('fundamental_analyses', 'Aucune analyse fondamentale disponible')}

**Prédictions:**
{data_dict.get('predictions_text', 'Aucune prédiction disponible')}

═══════════════════════════════════════════════════════════════

GÉNÈRE UN RAPPORT STRUCTURÉ EN FRANÇAIS AVEC CES 4 PARTIES:

**PARTIE 1 : ANALYSE DE L'ÉVOLUTION DU COURS (100 derniers jours)**

Rédige un paragraphe de 5-7 lignes analysant:
- Le pourcentage d'évolution total sur la période
- Le cours le plus haut et le plus bas atteints
- La tendance générale (haussière, baissière, stable)
- Les variations significatives observées
- Le contexte de volatilité

**PARTIE 2 : ANALYSE TECHNIQUE DÉTAILLÉE**

Pour CHAQUE indicateur, rédige un paragraphe de 2-3 lignes:
- **Moyennes Mobiles**: Interprète les valeurs MM20 et MM50, leur position relative au cours actuel, et justifie la décision
- **Bandes de Bollinger**: Explique la position du cours par rapport aux bornes, la volatilité, et justifie la décision
- **MACD**: Analyse la divergence MACD-Signal, le momentum, et justifie la décision
- **RSI**: Interprète la valeur (suracheté >70, survente <30, neutre 30-70) et justifie la décision
- **Stochastique**: Analyse %K et %D, leur croisement éventuel, et justifie la décision

Puis rédige une **conclusion technique** de 3-4 lignes synthétisant tous les indicateurs.

**PARTIE 3 : ANALYSE FONDAMENTALE**

Rédige un paragraphe de 6-8 lignes analysant:
- Les derniers résultats financiers (CA, résultat net, dividendes)
- L'évolution par rapport aux périodes précédentes
- Les perspectives et projets mentionnés
- La solidité financière globale
- La recommandation fondamentale (acheter/conserver/vendre) BASÉE UNIQUEMENT sur les fondamentaux
- IMPORTANT: Privilégie les rapports les plus récents et mentionne leur date

**PARTIE 4 : CONCLUSION D'INVESTISSEMENT**

Rédige un paragraphe de 5-6 lignes:
- Synthétise les 3 analyses précédentes
- Donne une recommandation finale claire: **ACHAT FORT**, **ACHAT**, **CONSERVER**, **VENTE**, ou **VENTE FORTE**
- Justifie par la convergence ou divergence des signaux
- Indique le niveau de confiance: Élevé, Moyen, ou Faible
- Mentionne le niveau de risque: Faible, Moyen, ou Élevé
- Suggère un horizon d'investissement (court, moyen, long terme)

═══════════════════════════════════════════════════════════════

IMPORTANT:
- Rédige en français professionnel
- Utilise des paragraphes fluides, pas de bullet points
- Sois précis avec les chiffres
- Reste factuel et objectif
- Si une donnée manque, mentionne-le clairement
- Commence chaque partie par son titre en gras"""

        # ✅ MISTRAL AI API
        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        request_body = {
            "model": MISTRAL_MODEL,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 3000,
            "temperature": 0.4
        }
        
        try:
            response = requests.post(MISTRAL_API_URL, headers=headers, json=request_body, timeout=60)
            
            self.request_count += 1
            
            if response.status_code == 200:
                data = response.json()
                if 'choices' in data and len(data['choices']) > 0:
                    text = data['choices'][0]['message']['content']
                    logging.info(f"    ✅ {symbol}: Analyse professionnelle générée")
                    return text
                else:
                    logging.warning(f"    ⚠️  Réponse vide pour {symbol}")
                    return self._generate_fallback_analysis(symbol, data_dict)
            
            elif response.status_code == 429:
                logging.warning(f"    ⚠️  Rate limit pour {symbol} (tentative {attempt}/{max_attempts})")
                
                if attempt < max_attempts:
                    time.sleep(10)
                    return self._generate_professional_analysis(symbol, data_dict, attempt + 1, max_attempts)
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
        """Analyse de secours structurée"""
        analysis = f"**ANALYSE DE {symbol}**\n\n"
        
        # Partie 1: Évolution du cours
        analysis += "**PARTIE 1 : ANALYSE DE L'ÉVOLUTION DU COURS (100 derniers jours)**\n\n"
        if data_dict.get('historical_summary'):
            analysis += f"{data_dict['historical_summary']}\n\n"
        else:
            analysis += "Les données historiques sur 100 jours ne sont pas disponibles pour cette action.\n\n"
        
        # Partie 2: Analyse technique
        analysis += "**PARTIE 2 : ANALYSE TECHNIQUE DÉTAILLÉE**\n\n"
        
        signals = []
        if data_dict.get('mm_decision'):
            signals.append(data_dict['mm_decision'])
            analysis += f"Les moyennes mobiles (MM20: {data_dict.get('mm_20', 'N/A')}, MM50: {data_dict.get('mm_50', 'N/A')}) suggèrent une tendance {data_dict['mm_decision'].lower()}. "
        
        if data_dict.get('bollinger_decision'):
            signals.append(data_dict['bollinger_decision'])
            analysis += f"Les bandes de Bollinger indiquent un signal {data_dict['bollinger_decision'].lower()}. "
        
        if data_dict.get('macd_decision'):
            signals.append(data_dict['macd_decision'])
            analysis += f"Le MACD confirme une position {data_dict['macd_decision'].lower()}. "
        
        if data_dict.get('rsi_decision'):
            signals.append(data_dict['rsi_decision'])
            analysis += f"Le RSI ({data_dict.get('rsi_value', 'N/A')}) signale {data_dict['rsi_decision'].lower()}. "
        
        if data_dict.get('stochastic_decision'):
            signals.append(data_dict['stochastic_decision'])
            analysis += f"Le stochastique recommande {data_dict['stochastic_decision'].lower()}.\n\n"
        
        # Conclusion technique
        buy_count = signals.count('Achat')
        sell_count = signals.count('Vente')
        
        if buy_count > sell_count:
            analysis += "**Conclusion technique**: Les indicateurs convergent majoritairement vers un signal d'achat.\n\n"
        elif sell_count > buy_count:
            analysis += "**Conclusion technique**: Les indicateurs convergent majoritairement vers un signal de vente.\n\n"
        else:
            analysis += "**Conclusion technique**: Les indicateurs sont mixtes, suggérant une position de conservation.\n\n"
        
        # Partie 3: Analyse fondamentale
        analysis += "**PARTIE 3 : ANALYSE FONDAMENTALE**\n\n"
        if data_dict.get('fundamental_analyses'):
            analysis += f"{data_dict['fundamental_analyses'][:500]}...\n\n"
        else:
            analysis += "Aucune analyse fondamentale récente n'est disponible pour cette société.\n\n"
        
        # Partie 4: Conclusion
        analysis += "**PARTIE 4 : CONCLUSION D'INVESTISSEMENT**\n\n"
        
        if buy_count > sell_count:
            analysis += f"**Recommandation: ACHAT**\n\nEn combinant l'analyse technique ({buy_count} signaux d'achat sur {len(signals)}) "
            analysis += "et les éléments fondamentaux disponibles, cette action présente des perspectives favorables. "
            analysis += "Niveau de confiance: Moyen. Niveau de risque: Moyen. Horizon: Moyen terme.\n"
        elif sell_count > buy_count:
            analysis += f"**Recommandation: VENTE**\n\nL'analyse technique ({sell_count} signaux de vente sur {len(signals)}) "
            analysis += "suggère une prudence. Il est recommandé d'envisager une sortie de position. "
            analysis += "Niveau de confiance: Moyen. Niveau de risque: Élevé. Horizon: Court terme.\n"
        else:
            analysis += "**Recommandation: CONSERVER**\n\nLes signaux techniques mixtes et l'absence d'éléments fondamentaux "
            analysis += "déterminants suggèrent de maintenir la position actuelle. "
            analysis += "Niveau de confiance: Faible. Niveau de risque: Moyen. Horizon: Moyen terme.\n"
        
        return analysis

    def _create_word_document(self, all_analyses):
        """Création du document Word professionnel"""
        logging.info("📄 Création du document Word professionnel...")
        
        doc = Document()
        
        # Style du document
        style = doc.styles['Normal']
        style.font.name = 'Calibri'
        style.font.size = Pt(11)
        
        # En-tête
        title = doc.add_heading('RAPPORT D\'ANALYSE BRVM', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        title_run = title.runs[0]
        title_run.font.color.rgb = RGBColor(0, 51, 102)
        
        subtitle = doc.add_paragraph(f"Rapport d'investissement professionnel")
        subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
        subtitle_run = subtitle.runs[0]
        subtitle_run.font.size = Pt(12)
        subtitle_run.font.color.rgb = RGBColor(64, 64, 64)
        
        date_p = doc.add_paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}")
        date_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        date_run = date_p.runs[0]
        date_run.font.size = Pt(10)
        date_run.font.italic = True
        
        doc.add_paragraph()
        
        # Avertissement
        warning = doc.add_paragraph()
        warning_run = warning.add_run("AVERTISSEMENT: ")
        warning_run.bold = True
        warning_run.font.color.rgb = RGBColor(192, 0, 0)
        warning.add_run("Ce rapport est généré automatiquement à partir d'analyses techniques et fondamentales. "
                       "Il ne constitue pas un conseil en investissement. Consultez un professionnel avant toute décision.")
        warning.paragraph_format.space_after = Pt(12)
        
        doc.add_page_break()
        
        # Synthèse
        doc.add_heading('SYNTHÈSE GÉNÉRALE', level=1)
        synth_p = doc.add_paragraph(f"Ce rapport présente une analyse détaillée de {len(all_analyses)} sociétés cotées "
                                    f"à la Bourse Régionale des Valeurs Mobilières (BRVM). Chaque analyse comprend "
                                    f"4 parties: évolution du cours, analyse technique, analyse fondamentale, et "
                                    f"conclusion d'investissement.")
        synth_p.paragraph_format.space_after = Pt(12)
        
        doc.add_page_break()
        
        # Table des matières
        doc.add_heading('TABLE DES MATIÈRES', level=1)
        for idx, symbol in enumerate(sorted(all_analyses.keys()), 1):
            toc_p = doc.add_paragraph(f"{idx}. {symbol}")
            toc_p.paragraph_format.left_indent = Pt(20)
        
        doc.add_page_break()
        
        # Analyses détaillées
        doc.add_heading('ANALYSES DÉTAILLÉES', level=1)
        
        for idx, (symbol, analysis) in enumerate(sorted(all_analyses.items()), 1):
            # Titre de la société
            company_heading = doc.add_heading(f"{idx}. {symbol}", level=2)
            company_heading.paragraph_format.space_before = Pt(18)
            company_heading_run = company_heading.runs[0]
            company_heading_run.font.color.rgb = RGBColor(0, 102, 204)
            
            # Ligne de séparation
            doc.add_paragraph("─" * 80)
            
            # Contenu de l'analyse
            # Diviser en paragraphes pour meilleure lisibilité
            paragraphs = analysis.split('\n\n')
            for para_text in paragraphs:
                if para_text.strip():
                    p = doc.add_paragraph(para_text.strip())
                    p.paragraph_format.space_after = Pt(6)
                    p.paragraph_format.line_spacing = 1.15
                    
                    # Mise en gras des titres de parties
                    if para_text.startswith('**PARTIE') or para_text.startswith('**CONCLUSION'):
                        p.runs[0].bold = True
                        p.runs[0].font.size = Pt(12)
                        p.runs[0].font.color.rgb = RGBColor(0, 51, 102)
                        p.paragraph_format.space_before = Pt(12)
            
            # Espacement entre sociétés
            doc.add_paragraph()
            doc.add_paragraph("═" * 80)
            
            # Page break tous les 2 sociétés pour meilleure lecture
            if idx % 2 == 0 and idx < len(all_analyses):
                doc.add_page_break()
        
        # Pied de page
        doc.add_page_break()
        footer = doc.add_heading('NOTES IMPORTANTES', level=1)
        footer_text = doc.add_paragraph(
            "1. Les analyses techniques sont basées sur les 5 indicateurs classiques (MM, Bollinger, MACD, RSI, Stochastique).\n"
            "2. Les analyses fondamentales proviennent des rapports financiers officiels publiés par les sociétés.\n"
            "3. Les recommandations sont générées par intelligence artificielle (Mistral AI).\n"
            "4. Tous les cours sont en FCFA (Francs CFA).\n"
            "5. Les prédictions sont des estimations basées sur des modèles statistiques et ne garantissent pas les performances futures.\n"
            "6. Ce document est confidentiel et destiné uniquement à l'usage du destinataire."
        )
        footer_text.paragraph_format.space_after = Pt(6)
        
        # Sauvegarde
        filename = f"Rapport_Professionnel_BRVM_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(filename)
        
        logging.info(f"   ✅ Document créé: {filename}")
        return filename

    def generate_all_reports(self, new_fundamental_analyses):
        """Génération du rapport complet professionnel"""
        logging.info("="*80)
        logging.info("📝 ÉTAPE 5: GÉNÉRATION RAPPORTS PROFESSIONNELS (V26.0 - Mistral AI)")
        logging.info(f"🤖 Modèle: {MISTRAL_MODEL}")
        logging.info("="*80)
        
        if not MISTRAL_API_KEY:
            logging.error("❌ Clé Mistral non configurée")
            return
        
        logging.info("✅ Clé Mistral chargée")
        
        # Récupération données
        df = self._get_all_data_from_db()
        
        if df.empty:
            logging.error("❌ Aucune donnée disponible - rapport impossible")
            return
        
        predictions_df = self._get_predictions_from_db()
        
        # Génération analyses professionnelles
        logging.info(f"🤖 Génération de {len(df)} analyse(s) professionnelle(s) détaillée(s)...")
        
        all_analyses = {}
        
        for idx, row in df.iterrows():
            symbol = row['symbol']
            company_id = row['company_id']
            
            # Récupérer l'historique 100 jours
            hist_df = self._get_historical_data_100days(company_id)
            
            historical_summary = "Données historiques non disponibles."
            if not hist_df.empty and len(hist_df) > 1:
                prix_debut = hist_df.iloc[0]['price']
                prix_fin = hist_df.iloc[-1]['price']
                prix_max = hist_df['price'].max()
                prix_min = hist_df['price'].min()
                evolution_pct = ((prix_fin - prix_debut) / prix_debut * 100) if prix_debut > 0 else 0
                
                historical_summary = (
                    f"Sur les 100 derniers jours, le cours a évolué de {evolution_pct:.2f}%, "
                    f"passant de {prix_debut:.0f} FCFA à {prix_fin:.0f} FCFA. "
                    f"Le cours le plus haut atteint est de {prix_max:.0f} FCFA et le plus bas de {prix_min:.0f} FCFA. "
                    f"Volume moyen échangé: {hist_df['volume'].mean():.0f} titres."
                )
            
            # Préparer les analyses fondamentales
            fundamental_text = ""
            if row.get('fundamental_summaries') and pd.notna(row['fundamental_summaries']):
                reports = row['fundamental_summaries'].split('###REPORT###')
                fundamental_parts = []
                for report in reports[:3]:  # Top 3 rapports les plus récents
                    if report.strip():
                        parts = report.split('|||')
                        if len(parts) == 3:
                            title, date, summary = parts
                            fundamental_parts.append(f"- {title} ({date}):\n{summary}")
                
                if fundamental_parts:
                    fundamental_text = "\n\n".join(fundamental_parts)
            
            # Préparer contexte complet
            data_dict = {
                'price': row.get('price'),
                'volume': row.get('volume'),
                'historical_summary': historical_summary,
                'mm_20': row.get('mm_20'),
                'mm_50': row.get('mm_50'),
                'mm_decision': row.get('mm_decision'),
                'bollinger_upper': row.get('bollinger_upper'),
                'bollinger_lower': row.get('bollinger_lower'),
                'bollinger_decision': row.get('bollinger_decision'),
                'macd_value': row.get('macd_value'),
                'macd_signal': row.get('macd_signal'),
                'macd_decision': row.get('macd_decision'),
                'rsi_value': row.get('rsi_value'),
                'rsi_decision': row.get('rsi_decision'),
                'stochastic_k': row.get('stochastic_k'),
                'stochastic_d': row.get('stochastic_d'),
                'stochastic_decision': row.get('stochastic_decision'),
                'fundamental_analyses': fundamental_text if fundamental_text else "Aucune analyse fondamentale récente disponible.",
                'predictions': []
            }
            
            # Prédictions
            symbol_predictions = predictions_df[predictions_df['symbol'] == symbol]
            if not symbol_predictions.empty:
                data_dict['predictions'] = [
                    {'date': row['prediction_date'], 'price': row['predicted_price']}
                    for _, row in symbol_predictions.head(10).iterrows()
                ]
                pred_list = [f"{p['date']}: {p['price']:.0f} FCFA" for p in data_dict['predictions'][:5]]
                data_dict['predictions_text'] = ", ".join(pred_list)
            else:
                data_dict['predictions_text'] = "Aucune prédiction disponible"
            
            # Génération analyse professionnelle
            analysis = self._generate_professional_analysis(symbol, data_dict)
            all_analyses[symbol] = analysis
        
        # Création document
        filename = self._create_word_document(all_analyses)
        
        logging.info(f"\n✅ Rapport professionnel généré: {filename}")
        logging.info(f"📊 Requêtes Mistral effectuées: {self.request_count}")

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
