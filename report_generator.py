# ==============================================================================
# MODULE: REPORT GENERATOR V29.0 ULTIMATE - ANALYSES AVANCÉES + MULTI-AI
# Toutes fonctionnalités V28 + Analyses sectorielles + Matrices + Liquidité
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
import json
from collections import defaultdict, Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION MULTI-AI (Rotation: DeepSeek → Gemini → Mistral)
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

MISTRAL_API_KEY = os.environ.get('MISTRAL_API_KEY')
MISTRAL_MODEL = "mistral-large-latest"
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"


class BRVMReportGenerator:
    def __init__(self):
        self.db_conn = None
        self.request_count = {'deepseek': 0, 'gemini': 0, 'mistral': 0, 'total': 0}
        self.all_recommendations = {}
        
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

    def _get_market_events(self):
        """Récupère les événements récents du marché"""
        logging.info("📰 Récupération des événements marquants...")
        
        query = """
        SELECT event_date, event_summary
        FROM new_market_events
        ORDER BY event_date DESC
        LIMIT 5;
        """
        
        try:
            df = pd.read_sql(query, self.db_conn)
            if not df.empty:
                events = []
                for _, row in df.iterrows():
                    events.append(f"• {row['event_date'].strftime('%d/%m/%Y')}: {row['event_summary']}")
                return "\n".join(events)
            return "Aucun événement récent enregistré."
        except Exception as e:
            logging.error(f"❌ Erreur récupération événements: {e}")
            return "Données indisponibles."

    def _get_market_indicators(self):
        """Récupère les derniers indicateurs du marché"""
        query = """
        SELECT 
            brvm_composite, 
            brvm_30, 
            brvm_prestige, 
            capitalisation_globale,
            variation_journaliere_brvm_composite,
            variation_ytd_brvm_composite
        FROM new_market_indicators
        ORDER BY extraction_date DESC
        LIMIT 1;
        """
        
        try:
            df = pd.read_sql(query, self.db_conn)
            if not df.empty:
                row = df.iloc[0]
                composite = row.get('brvm_composite')
                if pd.notna(composite):
                    return {
                        'composite': float(composite),
                        'composite_var_day': float(row.get('variation_journaliere_brvm_composite')) if pd.notna(row.get('variation_journaliere_brvm_composite')) else None,
                        'composite_var_ytd': float(row.get('variation_ytd_brvm_composite')) if pd.notna(row.get('variation_ytd_brvm_composite')) else None,
                        'capitalisation': float(row.get('capitalisation_globale')) if pd.notna(row.get('capitalisation_globale')) else None
                    }
            return None
        except Exception as e:
            logging.error(f"❌ Erreur récupération indicateurs: {e}")
            return None

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
        """Récupération optimisée des données avec NOMS des sociétés"""
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
            c.sector,
            lpc.trade_date, 
            lpc.price,
            lpc.volume,
            ta.mm20,
            ta.mm50,
            ta.mm_decision,
            ta.bollinger_superior,
            ta.bollinger_inferior,
            ta.bollinger_decision,
            ta.macd_line,
            ta.signal_line,
            ta.macd_decision,
            ta.rsi,
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

    def _extract_recommendation_from_analysis(self, analysis_text):
        """Extrait la recommandation du texte d'analyse"""
        analysis_lower = analysis_text.lower()
        
        if 'achat fort' in analysis_lower:
            return 'ACHAT FORT', 5
        elif 'achat' in analysis_lower:
            return 'ACHAT', 4
        elif 'conserver' in analysis_lower:
            return 'CONSERVER', 3
        elif 'vente forte' in analysis_lower:
            return 'VENTE FORTE', 1
        elif 'vente' in analysis_lower:
            return 'VENTE', 2
        else:
            return 'CONSERVER', 3

    # ============================================================================
    # NOUVELLES FONCTIONS MULTI-AI (DeepSeek → Gemini → Mistral)
    # ============================================================================
    
    def _generate_analysis_with_deepseek(self, symbol, data_dict, prompt):
        """Génération d'analyse avec DeepSeek"""
        if not DEEPSEEK_API_KEY:
            return None, None
        
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": DEEPSEEK_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 3000,
            "temperature": 0.4
        }
        
        try:
            response = requests.post(DEEPSEEK_API_URL, headers=headers, json=data, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    text = result['choices'][0]['message']['content']
                    self.request_count['deepseek'] += 1
                    self.request_count['total'] += 1
                    return text, "deepseek"
            
            return None, None
            
        except Exception as e:
            logging.error(f"❌ DeepSeek exception: {e}")
            return None, None

    def _generate_analysis_with_gemini(self, symbol, data_dict, prompt):
        """Génération d'analyse avec Gemini"""
        if not GEMINI_API_KEY:
            return None, None
        
        url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
        
        data = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "generationConfig": {
                "temperature": 0.4,
                "maxOutputTokens": 3000
            }
        }
        
        try:
            response = requests.post(url, json=data, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                if 'candidates' in result and len(result['candidates']) > 0:
                    text = result['candidates'][0]['content']['parts'][0]['text']
                    self.request_count['gemini'] += 1
                    self.request_count['total'] += 1
                    return text, "gemini"
            
            return None, None
            
        except Exception as e:
            logging.error(f"❌ Gemini exception: {e}")
            return None, None

    def _generate_analysis_with_mistral(self, symbol, data_dict, prompt):
        """Génération d'analyse avec Mistral"""
        if not MISTRAL_API_KEY:
            return None, None
        
        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        request_body = {
            "model": MISTRAL_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 3000,
            "temperature": 0.4
        }
        
        try:
            response = requests.post(MISTRAL_API_URL, headers=headers, json=request_body, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                if 'choices' in data and len(data['choices']) > 0:
                    text = data['choices'][0]['message']['content']
                    self.request_count['mistral'] += 1
                    self.request_count['total'] += 1
                    return text, "mistral"
            
            return None, None
                
        except Exception as e:
            logging.error(f"❌ Mistral exception: {e}")
            return None, None

    def _generate_professional_analysis(self, symbol, data_dict, attempt=1, max_attempts=3):
        """Génération analyse professionnelle avec rotation Multi-AI"""
        
        if attempt > 1:
            logging.info(f"    🔄 {symbol}: Tentative {attempt}/{max_attempts}")
        
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
        
        # ROTATION MULTI-AI: DeepSeek → Gemini → Mistral
        analysis = None
        provider = None
        
        # Tentative 1: DeepSeek
        logging.info(f"    🤖 {symbol}: Tentative DeepSeek...")
        analysis, provider = self._generate_analysis_with_deepseek(symbol, data_dict, prompt)
        
        if not analysis:
            # Tentative 2: Gemini
            logging.info(f"    🤖 {symbol}: Tentative Gemini...")
            analysis, provider = self._generate_analysis_with_gemini(symbol, data_dict, prompt)
        
        if not analysis:
            # Tentative 3: Mistral
            logging.info(f"    🤖 {symbol}: Tentative Mistral...")
            analysis, provider = self._generate_analysis_with_mistral(symbol, data_dict, prompt)
        
        # Si toutes les API ont échoué
        if not analysis:
            if attempt < max_attempts:
                logging.warning(f"    ⚠️  {symbol}: Toutes API échouées, retry {attempt+1}/{max_attempts}")
                time.sleep(10)
                return self._generate_professional_analysis(symbol, data_dict, attempt + 1, max_attempts)
            else:
                logging.error(f"    ❌ {symbol}: Échec définitif après {max_attempts} tentatives")
                return self._generate_fallback_analysis(symbol, data_dict)
        
        logging.info(f"    ✅ {symbol}: Analyse générée via {provider.upper()}")
        return analysis

    def _generate_fallback_analysis(self, symbol, data_dict):
        """Analyse de secours structurée"""
        analysis = f"**ANALYSE DE {symbol}**\n\n"
        
        analysis += "**PARTIE 1 : ANALYSE DE L'ÉVOLUTION DU COURS (100 derniers jours)**\n\n"
        if data_dict.get('historical_summary'):
            analysis += f"{data_dict['historical_summary']}\n\n"
        else:
            analysis += "Les données historiques sur 100 jours ne sont pas disponibles pour cette action.\n\n"
        
        analysis += "**PARTIE 2 : ANALYSE TECHNIQUE DÉTAILLÉE**\n\n"
        
        signals = []
        if data_dict.get('mm_decision'):
            signals.append(data_dict['mm_decision'])
            analysis += f"**Moyennes Mobiles**: Les moyennes mobiles (MM20: {data_dict.get('mm_20', 'N/A')}, MM50: {data_dict.get('mm_50', 'N/A')}) suggèrent une tendance {data_dict['mm_decision'].lower()}.\n\n"
        
        if data_dict.get('bollinger_decision'):
            signals.append(data_dict['bollinger_decision'])
            analysis += f"**Bandes de Bollinger**: Les bandes de Bollinger indiquent un signal {data_dict['bollinger_decision'].lower()}.\n\n"
        
        if data_dict.get('macd_decision'):
            signals.append(data_dict['macd_decision'])
            analysis += f"**MACD**: Le MACD confirme une position {data_dict['macd_decision'].lower()}.\n\n"
        
        if data_dict.get('rsi_decision'):
            signals.append(data_dict['rsi_decision'])
            analysis += f"**RSI**: Le RSI ({data_dict.get('rsi_value', 'N/A')}) signale {data_dict['rsi_decision'].lower()}.\n\n"
        
        if data_dict.get('stochastic_decision'):
            signals.append(data_dict['stochastic_decision'])
            analysis += f"**Stochastique**: Le stochastique recommande {data_dict['stochastic_decision'].lower()}.\n\n"
        
        buy_count = signals.count('Achat')
        sell_count = signals.count('Vente')
        
        if buy_count > sell_count:
            analysis += "**Conclusion technique**: Les indicateurs convergent majoritairement vers un signal d'achat.\n\n"
        elif sell_count > buy_count:
            analysis += "**Conclusion technique**: Les indicateurs convergent majoritairement vers un signal de vente.\n\n"
        else:
            analysis += "**Conclusion technique**: Les indicateurs sont mixtes, suggérant une position de conservation.\n\n"
        
        analysis += "**PARTIE 3 : ANALYSE FONDAMENTALE**\n\n"
        if data_dict.get('fundamental_analyses'):
            analysis += f"{data_dict['fundamental_analyses'][:500]}...\n\n"
        else:
            analysis += "Aucune analyse fondamentale récente n'est disponible pour cette société.\n\n"
        
        analysis += "**PARTIE 4 : CONCLUSION D'INVESTISSEMENT**\n\n"
        
        if buy_count > sell_count:
            analysis += f"**Recommandation: ACHAT**\n\nEn combinant l'analyse technique ({buy_count} signaux d'achat sur {len(signals)}) et les éléments fondamentaux disponibles, cette action présente des perspectives favorables. Niveau de confiance: Moyen. Niveau de risque: Moyen. Horizon: Moyen terme.\n"
        elif sell_count > buy_count:
            analysis += f"**Recommandation: VENTE**\n\nL'analyse technique ({sell_count} signaux de vente sur {len(signals)}) suggère une prudence. Il est recommandé d'envisager une sortie de position. Niveau de confiance: Moyen. Niveau de risque: Élevé. Horizon: Court terme.\n"
        else:
            analysis += "**Recommandation: CONSERVER**\n\nLes signaux techniques mixtes et l'absence d'éléments fondamentaux déterminants suggèrent de maintenir la position actuelle. Niveau de confiance: Faible. Niveau de risque: Moyen. Horizon: Moyen terme.\n"
        
        return analysis

    # ============================================================================
    # NOUVELLES ANALYSES AVANCÉES (V29.0)
    # ============================================================================
    
    def _calculate_sector_analysis(self, all_company_data):
        """1. ANALYSE PAR SECTEUR: Performance moyenne, sentiment, risque"""
        logging.info("📊 Calcul de l'analyse sectorielle...")
        
        sector_stats = defaultdict(lambda: {
            'companies': [],
            'prices': [],
            'performances': [],
            'recommendations': [],
            'risk_levels': [],
            'volumes': []
        })
        
        for symbol, data in all_company_data.items():
            sector = data.get('sector', 'Non classifié')
            if not sector or str(sector).strip() == '':
                sector = 'Non classifié'
            
            company_name = data.get('company_name', 'N/A')
            sector_stats[sector]['companies'].append(f"{symbol} ({company_name})")
            
            if data.get('price_evolution_100d') and pd.notna(data.get('price_evolution_100d')):
                sector_stats[sector]['performances'].append(data['price_evolution_100d'])
            
            if data.get('recommendation'):
                sector_stats[sector]['recommendations'].append(data['recommendation'])
            
            if data.get('risk_level'):
                sector_stats[sector]['risk_levels'].append(data['risk_level'])
            
            if data.get('current_price') and pd.notna(data.get('current_price')):
                sector_stats[sector]['prices'].append(data['current_price'])
        
        # Calculer les statistiques par secteur
        sector_analysis = {}
        for sector, stats in sector_stats.items():
            # Performance moyenne (avec gestion du cas vide)
            avg_perf = sum(stats['performances']) / len(stats['performances']) if stats['performances'] else 0
            
            # Sentiment général (recommandation la plus fréquente)
            rec_counter = Counter(stats['recommendations'])
            sentiment = rec_counter.most_common(1)[0][0] if rec_counter else 'NEUTRE'
            
            # Niveau de risque moyen
            risk_mapping = {'Faible': 1, 'Moyen': 2, 'Élevé': 3}
            risk_scores = [risk_mapping.get(r, 2) for r in stats['risk_levels']]
            avg_risk_score = sum(risk_scores) / len(risk_scores) if risk_scores else 2
            avg_risk = 'Faible' if avg_risk_score < 1.5 else 'Moyen' if avg_risk_score < 2.5 else 'Élevé'
            
            # Prix moyen (avec gestion du cas vide)
            prix_moyen = sum(stats['prices']) / len(stats['prices']) if stats['prices'] else 0
            
            sector_analysis[sector] = {
                'nb_societes': len(stats['companies']),
                'societes': stats['companies'],
                'performance_moyenne': avg_perf,
                'sentiment_general': sentiment,
                'risque_moyen': avg_risk,
                'prix_moyen': prix_moyen,
                'distribution_recommandations': dict(rec_counter)
            }
        
        return sector_analysis

    def _calculate_signal_convergence_matrix(self, all_company_data):
        """2. MATRICE DE CONVERGENCE: Signaux techniques vs fondamentaux"""
        logging.info("🔄 Calcul de la matrice de convergence...")
        
        matrix = {
            'convergence_forte': [],  # Tech=Achat Fort + Fond=Achat Fort
            'convergence_achat': [],  # Tech=Achat + Fond=Achat
            'convergence_vente': [],  # Tech=Vente + Fond=Vente
            'divergence_forte': [],   # Tech=Achat mais Fond=Vente ou inverse
            'divergence_moderee': [], # Tech=Achat mais Fond=Neutre
            'neutre': []              # Signaux mixtes
        }
        
        for symbol, data in all_company_data.items():
            company_name = data.get('company_name', 'N/A')
            full_name = f"{symbol} ({company_name})"
            
            # Signal technique (basé sur la moyenne des indicateurs)
            tech_signals = []
            if data.get('mm_decision'): tech_signals.append(data['mm_decision'])
            if data.get('bollinger_decision'): tech_signals.append(data['bollinger_decision'])
            if data.get('macd_decision'): tech_signals.append(data['macd_decision'])
            if data.get('rsi_decision'): tech_signals.append(data['rsi_decision'])
            if data.get('stochastic_decision'): tech_signals.append(data['stochastic_decision'])
            
            buy_tech = sum(1 for s in tech_signals if 'Achat' in str(s))
            sell_tech = sum(1 for s in tech_signals if 'Vente' in str(s))
            
            if buy_tech > sell_tech:
                tech_signal = 'ACHAT'
            elif sell_tech > buy_tech:
                tech_signal = 'VENTE'
            else:
                tech_signal = 'NEUTRE'
            
            # Signal fondamental (extrait de la recommandation finale)
            final_rec = data.get('recommendation', 'CONSERVER')
            if 'ACHAT' in final_rec:
                fund_signal = 'ACHAT'
            elif 'VENTE' in final_rec:
                fund_signal = 'VENTE'
            else:
                fund_signal = 'NEUTRE'
            
            # Classement dans la matrice
            if tech_signal == 'ACHAT' and fund_signal == 'ACHAT':
                if 'FORT' in final_rec:
                    matrix['convergence_forte'].append(full_name)
                else:
                    matrix['convergence_achat'].append(full_name)
            elif tech_signal == 'VENTE' and fund_signal == 'VENTE':
                matrix['convergence_vente'].append(full_name)
            elif (tech_signal == 'ACHAT' and fund_signal == 'VENTE') or (tech_signal == 'VENTE' and fund_signal == 'ACHAT'):
                matrix['divergence_forte'].append(full_name)
            elif tech_signal != fund_signal and (tech_signal == 'NEUTRE' or fund_signal == 'NEUTRE'):
                matrix['divergence_moderee'].append(full_name)
            else:
                matrix['neutre'].append(full_name)
        
        return matrix

    def _calculate_liquidity_analysis(self, all_company_data):
        """3. ANALYSE DE LIQUIDITÉ: Volumes moyens et relation avec performance"""
        logging.info("💧 Calcul de l'analyse de liquidité...")
        
        # Récupérer les volumes moyens sur 30 jours pour chaque société
        liquidity_data = []
        
        for symbol, data in all_company_data.items():
            company_id = data.get('company_id')
            company_name = data.get('company_name', 'N/A')
            
            if not company_id:
                continue
            
            # Calculer le volume moyen sur 30 jours
            query = f"""
            SELECT AVG(volume) as avg_volume, AVG(value) as avg_value
            FROM historical_data
            WHERE company_id = {company_id}
              AND trade_date >= CURRENT_DATE - INTERVAL '30 days'
            """
            
            try:
                df = pd.read_sql(query, self.db_conn)
                if not df.empty:
                    avg_volume = df.iloc[0]['avg_volume'] if pd.notna(df.iloc[0]['avg_volume']) else 0
                    avg_value = df.iloc[0]['avg_value'] if pd.notna(df.iloc[0]['avg_value']) else 0
                    
                    # Ne garder que les sociétés avec des données de volume
                    if avg_volume > 0:
                        liquidity_data.append({
                            'symbol': symbol,
                            'company_name': company_name,
                            'avg_volume': avg_volume,
                            'avg_value': avg_value,
                            'performance': data.get('price_evolution_100d', 0) if pd.notna(data.get('price_evolution_100d')) else 0,
                            'recommendation': data.get('recommendation', 'N/A'),
                            'risk_level': data.get('risk_level', 'N/A')
                        })
            except Exception as e:
                logging.error(f"❌ Erreur volume pour {symbol}: {e}")
        
        # Trier par volume décroissant
        liquidity_data.sort(key=lambda x: x['avg_volume'], reverse=True)
        
        # Classifier en catégories (avec protection contre les listes vides)
        total = len(liquidity_data)
        if total == 0:
            return {
                'high_liquidity': [],
                'medium_liquidity': [],
                'low_liquidity': [],
                'all_data': []
            }
        
        # Assurer au moins 1 élément par catégorie si possible
        high_count = max(1, int(total * 0.2)) if total >= 5 else total
        medium_start = high_count
        medium_end = max(medium_start + 1, int(total * 0.6)) if total >= 5 else total
        
        high_liquidity = liquidity_data[:high_count]
        medium_liquidity = liquidity_data[medium_start:medium_end] if total > high_count else []
        low_liquidity = liquidity_data[medium_end:] if total > medium_end else []
        
        return {
            'high_liquidity': high_liquidity,
            'medium_liquidity': medium_liquidity,
            'low_liquidity': low_liquidity,
            'all_data': liquidity_data
        }

    def _calculate_top_divergences(self, all_company_data):
        """4. TOP 10 DES DIVERGENCES: Sociétés avec signaux contradictoires"""
        logging.info("⚠️  Calcul des divergences majeures...")
        
        divergences = []
        
        for symbol, data in all_company_data.items():
            company_name = data.get('company_name', 'N/A')
            
            # Compter signaux d'achat vs vente
            signals = {
                'MM': data.get('mm_decision'),
                'Bollinger': data.get('bollinger_decision'),
                'MACD': data.get('macd_decision'),
                'RSI': data.get('rsi_decision'),
                'Stochastique': data.get('stochastic_decision')
            }
            
            buy_signals = [k for k, v in signals.items() if v and 'Achat' in str(v)]
            sell_signals = [k for k, v in signals.items() if v and 'Vente' in str(v)]
            
            # Score de divergence (écart entre signaux)
            divergence_score = abs(len(buy_signals) - len(sell_signals))
            
            # Divergence avec fondamental
            final_rec = data.get('recommendation', 'CONSERVER')
            fund_is_buy = 'ACHAT' in final_rec
            fund_is_sell = 'VENTE' in final_rec
            
            tech_majority_buy = len(buy_signals) > len(sell_signals)
            tech_majority_sell = len(sell_signals) > len(buy_signals)
            
            if (tech_majority_buy and fund_is_sell) or (tech_majority_sell and fund_is_buy):
                divergence_score += 3  # Bonus si divergence tech/fondamental
            
            if divergence_score > 0:
                divergences.append({
                    'symbol': symbol,
                    'company_name': company_name,
                    'divergence_score': divergence_score,
                    'buy_signals': buy_signals,
                    'sell_signals': sell_signals,
                    'final_recommendation': final_rec,
                    'description': self._describe_divergence(buy_signals, sell_signals, final_rec)
                })
        
        # Trier par score décroissant
        divergences.sort(key=lambda x: x['divergence_score'], reverse=True)
        
        return divergences[:10]  # Top 10

    def _describe_divergence(self, buy_signals, sell_signals, final_rec):
        """Génère une description textuelle de la divergence"""
        desc = []
        
        if buy_signals:
            desc.append(f"Signaux d'achat: {', '.join(buy_signals)}")
        
        if sell_signals:
            desc.append(f"Signaux de vente: {', '.join(sell_signals)}")
        
        desc.append(f"Recommandation finale: {final_rec}")
        
        return " | ".join(desc)

    def _calculate_risk_horizon_matrix(self, all_company_data):
        """5. MATRICE RISQUE vs HORIZON"""
        logging.info("📈 Calcul de la matrice Risque/Horizon...")
        
        matrix = {
            'faible_court': [],
            'faible_moyen': [],
            'faible_long': [],
            'moyen_court': [],
            'moyen_moyen': [],
            'moyen_long': [],
            'eleve_court': [],
            'eleve_moyen': [],
            'eleve_long': []
        }
        
        for symbol, data in all_company_data.items():
            company_name = data.get('company_name', 'N/A')
            full_name = f"{symbol} ({company_name})"
            
            risk = data.get('risk_level', 'Moyen').lower()
            horizon = data.get('investment_horizon', 'Moyen terme').lower()
            
            # Normaliser horizon
            if 'court' in horizon:
                horizon_cat = 'court'
            elif 'long' in horizon:
                horizon_cat = 'long'
            else:
                horizon_cat = 'moyen'
            
            # Normaliser risque
            if 'faible' in risk:
                risk_cat = 'faible'
            elif 'élevé' in risk or 'eleve' in risk:
                risk_cat = 'eleve'
            else:
                risk_cat = 'moyen'
            
            key = f"{risk_cat}_{horizon_cat}"
            if key in matrix:
                matrix[key].append(full_name)
        
        return matrix

    # ============================================================================
    # SAUVEGARDE ET GÉNÉRATION DOCUMENT
    # ============================================================================
    
    def _save_to_database(self, report_date, synthesis_text, top_10, flop_10, market_events, all_company_data, filename):
        """Sauvegarde structurée dans la base de données"""
        logging.info("💾 Sauvegarde dans la base de données...")
        
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO report_summary (
                        report_date, synthesis_text, top_10_buy, flop_10_sell, 
                        market_events, total_companies_analyzed, report_file_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (report_date) DO UPDATE SET
                        synthesis_text = EXCLUDED.synthesis_text,
                        top_10_buy = EXCLUDED.top_10_buy,
                        flop_10_sell = EXCLUDED.flop_10_sell,
                        market_events = EXCLUDED.market_events,
                        total_companies_analyzed = EXCLUDED.total_companies_analyzed,
                        report_file_name = EXCLUDED.report_file_name
                    RETURNING id;
                """, (
                    report_date,
                    synthesis_text,
                    json.dumps(top_10, ensure_ascii=False),
                    json.dumps(flop_10, ensure_ascii=False),
                    market_events,
                    len(all_company_data),
                    filename
                ))
                
                report_summary_id = cur.fetchone()[0]
                
                for symbol, company_data in all_company_data.items():
                    cur.execute("""
                        INSERT INTO report_company_analysis (
                            report_summary_id, company_id, symbol, company_name,
                            current_price, price_evolution_100d, highest_price_100d, lowest_price_100d,
                            mm20, mm50, mm_decision,
                            bollinger_upper, bollinger_lower, bollinger_decision,
                            macd_value, macd_signal, macd_decision,
                            rsi_value, rsi_decision,
                            stochastic_k, stochastic_d, stochastic_decision,
                            price_evolution_text, technical_conclusion, fundamental_analysis, investment_conclusion,
                            recommendation, confidence_level, risk_level, investment_horizon
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s
                        )
                        ON CONFLICT (report_summary_id, symbol) DO UPDATE SET
                            current_price = EXCLUDED.current_price,
                            recommendation = EXCLUDED.recommendation,
                            investment_conclusion = EXCLUDED.investment_conclusion;
                    """, (
                        report_summary_id,
                        company_data.get('company_id'),
                        symbol,
                        company_data.get('company_name'),
                        company_data.get('current_price'),
                        company_data.get('price_evolution_100d'),
                        company_data.get('highest_price_100d'),
                        company_data.get('lowest_price_100d'),
                        company_data.get('mm20'),
                        company_data.get('mm50'),
                        company_data.get('mm_decision'),
                        company_data.get('bollinger_upper'),
                        company_data.get('bollinger_lower'),
                        company_data.get('bollinger_decision'),
                        company_data.get('macd_value'),
                        company_data.get('macd_signal'),
                        company_data.get('macd_decision'),
                        company_data.get('rsi_value'),
                        company_data.get('rsi_decision'),
                        company_data.get('stochastic_k'),
                        company_data.get('stochastic_d'),
                        company_data.get('stochastic_decision'),
                        company_data.get('full_analysis', ''),
                        company_data.get('technical_conclusion', ''),
                        company_data.get('fundamental_analysis', ''),
                        company_data.get('investment_conclusion', ''),
                        company_data.get('recommendation'),
                        company_data.get('confidence_level'),
                        company_data.get('risk_level'),
                        company_data.get('investment_horizon')
                    ))
                
                self.db_conn.commit()
                logging.info(f"   ✅ Rapport sauvegardé (ID: {report_summary_id})")
                
        except Exception as e:
            logging.error(f"❌ Erreur sauvegarde DB: {e}")
            self.db_conn.rollback()

    def _add_table_with_shading(self, doc, data, headers, column_widths=None):
        """Ajoute un tableau avec mise en forme"""
        table = doc.add_table(rows=1, cols=len(headers))
        table.style = 'Light Grid Accent 1'
        
        # En-têtes
        hdr_cells = table.rows[0].cells
        for i, header in enumerate(headers):
            hdr_cells[i].text = header
            hdr_cells[i].paragraphs[0].runs[0].font.bold = True
            # Couleur de fond grise pour l'en-tête
            shading_elm = OxmlElement('w:shd')
            shading_elm.set(qn('w:fill'), 'D9D9D9')
            hdr_cells[i]._element.get_or_add_tcPr().append(shading_elm)
        
        # Données
        for row_data in data:
            row = table.add_row()
            for i, value in enumerate(row_data):
                row.cells[i].text = str(value)
        
        return table

    def _create_word_document(self, all_analyses, all_company_data):
        """Création du document Word professionnel ULTRA-COMPLET"""
        logging.info("📄 Création du document Word ULTIMATE...")
        
        doc = Document()
        
        style = doc.styles['Normal']
        style.font.name = 'Calibri'
        style.font.size = Pt(11)
        
        # ========== PAGE DE TITRE ==========
        title = doc.add_heading('RAPPORT D\'ANALYSE BRVM', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        title_run = title.runs[0]
        title_run.font.color.rgb = RGBColor(0, 51, 102)
        
        subtitle = doc.add_paragraph(f"Rapport d'investissement professionnel - Édition Ultimate")
        subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
        subtitle_run = subtitle.runs[0]
        subtitle_run.font.size = Pt(12)
        subtitle_run.font.color.rgb = RGBColor(64, 64, 64)
        
        date_p = doc.add_paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}")
        date_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        date_run = date_p.runs[0]
        date_run.font.size = Pt(10)
        date_run.font.italic = True
        
        version_p = doc.add_paragraph(f"Version 29.0 - Analyses Multi-AI (DeepSeek + Gemini + Mistral)")
        version_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        version_run = version_p.runs[0]
        version_run.font.size = Pt(9)
        version_run.font.italic = True
        version_run.font.color.rgb = RGBColor(128, 128, 128)
        
        doc.add_paragraph()
        doc.add_page_break()
        
        # ========== SYNTHÈSE GÉNÉRALE ==========
        doc.add_heading('SYNTHÈSE GÉNÉRALE', level=1)
        
        market_indicators = self._get_market_indicators()
        intro = doc.add_paragraph(
            f"Ce rapport présente une analyse détaillée de {len(all_analyses)} sociétés cotées "
            f"à la Bourse Régionale des Valeurs Mobilières (BRVM). "
        )
        
        if market_indicators and market_indicators.get('composite'):
            intro.add_run(f"L'indice BRVM Composite s'établit à {market_indicators['composite']:.2f} points ")
            
            if market_indicators.get('composite_var_day') is not None:
                var_day = market_indicators['composite_var_day']
                if var_day > 0:
                    run = intro.add_run(f"(+{var_day:.2f}%)")
                    run.font.color.rgb = RGBColor(0, 128, 0)
                else:
                    run = intro.add_run(f"({var_day:.2f}%)")
                    run.font.color.rgb = RGBColor(192, 0, 0)
                intro.add_run(f" sur la séance. ")
            
            if market_indicators.get('capitalisation'):
                intro.add_run(f"La capitalisation globale du marché atteint {market_indicators['capitalisation']/1e9:.2f} milliards FCFA.")
        else:
            intro.add_run("Les indicateurs de marché seront mis à jour prochainement.")
        
        intro.paragraph_format.space_after = Pt(12)
        doc.add_paragraph()
        
        # ========== TOP 10 ACHATS ==========
        doc.add_heading('📈 TOP 10 DES OPPORTUNITÉS D\'ACHAT', level=2)
        
        sorted_buy = sorted(
            [(symbol, data) for symbol, data in all_company_data.items()],
            key=lambda x: x[1].get('recommendation_score', 0),
            reverse=True
        )[:10]
        
        top_10_list = []
        for idx, (symbol, data) in enumerate(sorted_buy, 1):
            p = doc.add_paragraph(style='List Number')
            company_name = data.get('company_name', 'N/A')
            p.add_run(f"{symbol} - {company_name}").bold = True
            p.add_run(f" | Prix: {data.get('current_price', 0):.0f} FCFA | ")
            
            rec_run = p.add_run(f"{data.get('recommendation', 'N/A')}")
            rec_run.font.color.rgb = RGBColor(0, 128, 0)
            rec_run.bold = True
            
            p.add_run(f" | Confiance: {data.get('confidence_level', 'N/A')} | Risque: {data.get('risk_level', 'N/A')}")
            
            top_10_list.append({
                'symbol': symbol,
                'name': company_name,
                'price': float(data.get('current_price', 0)),
                'recommendation': data.get('recommendation'),
                'confidence': data.get('confidence_level'),
                'risk': data.get('risk_level')
            })
        
        doc.add_paragraph()
        
        # ========== FLOP 10 VENTES ==========
        doc.add_heading('📉 TOP 10 DES ACTIONS À ÉVITER', level=2)
        
        # Tri CROISSANT pour avoir les pires scores (VENTE FORTE = 1, VENTE = 2)
        sorted_sell = sorted(
            [(symbol, data) for symbol, data in all_company_data.items()],
            key=lambda x: x[1].get('recommendation_score', 3)  # 3 = CONSERVER par défaut
        )[:10]
        
        flop_10_list = []
        for idx, (symbol, data) in enumerate(sorted_sell, 1):
            p = doc.add_paragraph(style='List Number')
            company_name = data.get('company_name', 'N/A')
            p.add_run(f"{symbol} - {company_name}").bold = True
            p.add_run(f" | Prix: {data.get('current_price', 0):.0f} FCFA | ")
            
            rec_run = p.add_run(f"{data.get('recommendation', 'N/A')}")
            rec_run.font.color.rgb = RGBColor(192, 0, 0)
            rec_run.bold = True
            
            p.add_run(f" | Confiance: {data.get('confidence_level', 'N/A')} | Risque: {data.get('risk_level', 'N/A')}")
            
            flop_10_list.append({
                'symbol': symbol,
                'name': company_name,
                'price': float(data.get('current_price', 0)),
                'recommendation': data.get('recommendation'),
                'confidence': data.get('confidence_level'),
                'risk': data.get('risk_level')
            })
        
        doc.add_paragraph()
        doc.add_page_break()
        
        # ========== 1. ANALYSE PAR SECTEUR ==========
        doc.add_heading('📊 ANALYSE PAR SECTEUR', level=1)
        
        sector_analysis = self._calculate_sector_analysis(all_company_data)
        
        doc.add_paragraph(
            "Cette section présente une analyse comparative de tous les secteurs représentés à la BRVM, "
            "incluant la performance moyenne, le sentiment général du marché et le niveau de risque moyen."
        )
        doc.add_paragraph()
        
        for sector, stats in sorted(sector_analysis.items(), key=lambda x: x[1]['performance_moyenne'], reverse=True):
            doc.add_heading(f"Secteur: {sector}", level=3)
            
            p = doc.add_paragraph()
            p.add_run(f"Nombre de sociétés: ").bold = True
            p.add_run(f"{stats['nb_societes']}\n")
            
            p.add_run(f"Performance moyenne (100j): ").bold = True
            perf_run = p.add_run(f"{stats['performance_moyenne']:.2f}%\n")
            if stats['performance_moyenne'] > 0:
                perf_run.font.color.rgb = RGBColor(0, 128, 0)
            else:
                perf_run.font.color.rgb = RGBColor(192, 0, 0)
            
            p.add_run(f"Sentiment général: ").bold = True
            p.add_run(f"{stats['sentiment_general']}\n")
            
            p.add_run(f"Risque moyen: ").bold = True
            p.add_run(f"{stats['risque_moyen']}\n")
            
            p.add_run(f"Prix moyen: ").bold = True
            p.add_run(f"{stats['prix_moyen']:.0f} FCFA\n")
            
            # Liste des sociétés du secteur
            p.add_run(f"\nSociétés: ").bold = True
            p.add_run(", ".join(stats['societes']))
            
            doc.add_paragraph()
        
        doc.add_page_break()
        
        # ========== 2. MATRICE DE CONVERGENCE ==========
        doc.add_heading('🔄 MATRICE DE CONVERGENCE DES SIGNAUX', level=1)
        
        convergence_matrix = self._calculate_signal_convergence_matrix(all_company_data)
        
        doc.add_paragraph(
            "Cette matrice identifie les sociétés présentant une convergence ou une divergence "
            "entre les signaux techniques et fondamentaux."
        )
        doc.add_paragraph()
        
        # Convergence forte
        doc.add_heading('✅ Convergence Forte (Technique + Fondamental = ACHAT FORT)', level=3)
        if convergence_matrix['convergence_forte']:
            for company in convergence_matrix['convergence_forte']:
                doc.add_paragraph(f"• {company}", style='List Bullet')
        else:
            doc.add_paragraph("Aucune société dans cette catégorie.")
        doc.add_paragraph()
        
        # Convergence achat
        doc.add_heading('✅ Convergence Achat (Technique + Fondamental = ACHAT)', level=3)
        if convergence_matrix['convergence_achat']:
            for company in convergence_matrix['convergence_achat']:
                doc.add_paragraph(f"• {company}", style='List Bullet')
        else:
            doc.add_paragraph("Aucune société dans cette catégorie.")
        doc.add_paragraph()
        
        # Convergence vente
        doc.add_heading('⚠️ Convergence Vente (Technique + Fondamental = VENTE)', level=3)
        if convergence_matrix['convergence_vente']:
            for company in convergence_matrix['convergence_vente']:
                doc.add_paragraph(f"• {company}", style='List Bullet')
        else:
            doc.add_paragraph("Aucune société dans cette catégorie.")
        doc.add_paragraph()
        
        # Divergence forte
        doc.add_heading('❌ Divergence Forte (Signaux opposés)', level=3)
        if convergence_matrix['divergence_forte']:
            for company in convergence_matrix['divergence_forte']:
                doc.add_paragraph(f"• {company}", style='List Bullet')
        else:
            doc.add_paragraph("Aucune société dans cette catégorie.")
        doc.add_paragraph()
        
        # Divergence modérée
        doc.add_heading('⚡ Divergence Modérée', level=3)
        if convergence_matrix['divergence_moderee']:
            for company in convergence_matrix['divergence_moderee']:
                doc.add_paragraph(f"• {company}", style='List Bullet')
        else:
            doc.add_paragraph("Aucune société dans cette catégorie.")
        
        doc.add_page_break()
        
        # ========== 3. ANALYSE DE LIQUIDITÉ ==========
        doc.add_heading('💧 ANALYSE DE LIQUIDITÉ', level=1)
        
        liquidity_analysis = self._calculate_liquidity_analysis(all_company_data)
        
        doc.add_paragraph(
            "Cette analyse compare les volumes moyens échangés pour identifier les titres les plus liquides "
            "(faciles à acheter/vendre) et ceux présentant un risque de liquidité."
        )
        doc.add_paragraph()
        
        # Haute liquidité
        doc.add_heading('🟢 Titres à Haute Liquidité (Top 20%)', level=3)
        high_liq_data = []
        for item in liquidity_analysis['high_liquidity']:
            high_liq_data.append([
                f"{item['symbol']} ({item['company_name']})",
                f"{item['avg_volume']:.0f}",
                f"{item['avg_value']:.0f} FCFA",
                f"{item['performance']:.2f}%",
                item['recommendation']
            ])
        
        if high_liq_data:
            self._add_table_with_shading(doc, high_liq_data, 
                                        ['Société', 'Vol. Moy.', 'Valeur Moy.', 'Perf. 100j', 'Recom.'])
        else:
            doc.add_paragraph("ℹ️ Données insuffisantes pour calculer la liquidité (besoin de 30 jours minimum).")
        doc.add_paragraph()
        
        # Faible liquidité
        doc.add_heading('🔴 Titres à Faible Liquidité (Bottom 40%) - RISQUE ÉLEVÉ', level=3)
        low_liq_data = []
        for item in liquidity_analysis['low_liquidity'][:10]:  # Top 10 des moins liquides
            low_liq_data.append([
                f"{item['symbol']} ({item['company_name']})",
                f"{item['avg_volume']:.0f}",
                f"{item['avg_value']:.0f} FCFA",
                f"{item['performance']:.2f}%",
                item['recommendation']
            ])
        
        if low_liq_data:
            self._add_table_with_shading(doc, low_liq_data, 
                                        ['Société', 'Vol. Moy.', 'Valeur Moy.', 'Perf. 100j', 'Recom.'])
        else:
            doc.add_paragraph("ℹ️ Données insuffisantes pour identifier les titres à faible liquidité.")
        
        doc.add_paragraph()
        p_warning = doc.add_paragraph()
        p_warning.add_run("⚠️ ATTENTION: ").bold = True
        p_warning.add_run(
            "Les titres à faible liquidité présentent un risque de ne pas pouvoir sortir facilement "
            "de sa position. À privilégier uniquement pour un investissement de long terme."
        )
        
        doc.add_page_break()
        
        # ========== 4. TOP 10 DIVERGENCES ==========
        doc.add_heading('⚠️ TOP 10 DES DIVERGENCES MAJEURES', level=1)
        
        top_divergences = self._calculate_top_divergences(all_company_data)
        
        doc.add_paragraph(
            "Cette section liste les sociétés présentant les écarts les plus importants "
            "entre les différents indicateurs techniques et fondamentaux."
        )
        doc.add_paragraph()
        
        for idx, div in enumerate(top_divergences, 1):
            doc.add_heading(f"{idx}. {div['symbol']} - {div['company_name']}", level=3)
            
            p = doc.add_paragraph()
            p.add_run(f"Score de divergence: ").bold = True
            p.add_run(f"{div['divergence_score']}/8\n")
            
            p.add_run(f"Description: ").bold = True
            p.add_run(f"{div['description']}\n")
            
            doc.add_paragraph()
        
        doc.add_page_break()
        
        # ========== 5. MATRICE RISQUE vs HORIZON ==========
        doc.add_heading('📈 MATRICE RISQUE vs HORIZON DE PLACEMENT', level=1)
        
        risk_horizon_matrix = self._calculate_risk_horizon_matrix(all_company_data)
        
        doc.add_paragraph(
            "Cette matrice croise le niveau de risque avec l'horizon de placement recommandé "
            "pour faciliter la sélection de titres selon votre profil d'investisseur."
        )
        doc.add_paragraph()
        
        # Tableau récapitulatif
        matrix_data = [
            ['RISQUE / HORIZON', 'Court Terme', 'Moyen Terme', 'Long Terme'],
            [
                'Risque Faible',
                str(len(risk_horizon_matrix['faible_court'])),
                str(len(risk_horizon_matrix['faible_moyen'])),
                str(len(risk_horizon_matrix['faible_long']))
            ],
            [
                'Risque Moyen',
                str(len(risk_horizon_matrix['moyen_court'])),
                str(len(risk_horizon_matrix['moyen_moyen'])),
                str(len(risk_horizon_matrix['moyen_long']))
            ],
            [
                'Risque Élevé',
                str(len(risk_horizon_matrix['eleve_court'])),
                str(len(risk_horizon_matrix['eleve_moyen'])),
                str(len(risk_horizon_matrix['eleve_long']))
            ]
        ]
        
        self._add_table_with_shading(doc, matrix_data[1:], matrix_data[0])
        doc.add_paragraph()
        
        # Détail par catégorie
        categories = [
            ('Faible Risque - Court Terme', 'faible_court', '🟢'),
            ('Faible Risque - Moyen Terme', 'faible_moyen', '🟢'),
            ('Faible Risque - Long Terme', 'faible_long', '🟢'),
            ('Risque Moyen - Court Terme', 'moyen_court', '🟡'),
            ('Risque Moyen - Moyen Terme', 'moyen_moyen', '🟡'),
            ('Risque Moyen - Long Terme', 'moyen_long', '🟡'),
            ('Risque Élevé - Court Terme', 'eleve_court', '🔴'),
            ('Risque Élevé - Moyen Terme', 'eleve_moyen', '🔴'),
            ('Risque Élevé - Long Terme', 'eleve_long', '🔴'),
        ]
        
        for cat_name, cat_key, emoji in categories:
            if risk_horizon_matrix[cat_key]:
                doc.add_heading(f"{emoji} {cat_name}", level=3)
                for company in risk_horizon_matrix[cat_key]:
                    doc.add_paragraph(f"• {company}", style='List Bullet')
                doc.add_paragraph()
        
        doc.add_page_break()
        
        # ========== ÉVÉNEMENTS MARQUANTS ==========
        doc.add_heading('📰 FAITS MARQUANTS RÉCENTS', level=2)
        market_events = self._get_market_events()
        events_p = doc.add_paragraph(market_events)
        events_p.paragraph_format.space_after = Pt(12)
        
        doc.add_page_break()
        
        # ========== TABLE DES MATIÈRES ==========
        doc.add_heading('TABLE DES MATIÈRES - ANALYSES DÉTAILLÉES', level=1)
        for idx, (symbol, data) in enumerate(sorted(all_company_data.items()), 1):
            company_name = data.get('company_name', 'N/A')
            toc_p = doc.add_paragraph(f"{idx}. {symbol} - {company_name}")
            toc_p.paragraph_format.left_indent = Pt(20)
        
        doc.add_page_break()
        
        # ========== ANALYSES DÉTAILLÉES ==========
        doc.add_heading('ANALYSES DÉTAILLÉES PAR SOCIÉTÉ', level=1)
        
        for idx, (symbol, analysis) in enumerate(sorted(all_analyses.items()), 1):
            company_data = all_company_data.get(symbol, {})
            company_name = company_data.get('company_name', 'N/A')
            
            company_heading = doc.add_heading(f"{idx}. {symbol} - {company_name}", level=2)
            company_heading.paragraph_format.space_before = Pt(18)
            company_heading_run = company_heading.runs[0]
            company_heading_run.font.color.rgb = RGBColor(0, 102, 204)
            
            doc.add_paragraph("─" * 80)
            
            paragraphs = analysis.split('\n\n')
            for para_text in paragraphs:
                if para_text.strip():
                    p = doc.add_paragraph(para_text.strip())
                    p.paragraph_format.space_after = Pt(6)
                    p.paragraph_format.line_spacing = 1.15
                    
                    if para_text.startswith('**PARTIE') or para_text.startswith('**CONCLUSION'):
                        p.runs[0].bold = True
                        p.runs[0].font.size = Pt(12)
                        p.runs[0].font.color.rgb = RGBColor(0, 51, 102)
                        p.paragraph_format.space_before = Pt(12)
            
            doc.add_paragraph()
            doc.add_paragraph("═" * 80)
            
            if idx % 2 == 0 and idx < len(all_analyses):
                doc.add_page_break()
        
        # ========== PIED DE PAGE ==========
        doc.add_page_break()
        footer = doc.add_heading('NOTES IMPORTANTES', level=1)
        footer_text = doc.add_paragraph(
            "1. Les analyses techniques sont basées sur les 5 indicateurs classiques (MM, Bollinger, MACD, RSI, Stochastique).\n"
            "2. Les analyses fondamentales proviennent des rapports financiers officiels de la BRVM.\n"
            "3. Les recommandations sont générées par intelligence artificielle Multi-AI (DeepSeek, Gemini, Mistral) avec rotation automatique.\n"
            "4. Tous les cours et valeurs sont exprimés en FCFA (Francs CFA).\n"
            "5. Les prédictions sont des estimations statistiques et ne garantissent pas les performances futures.\n"
            "6. L'analyse de liquidité est basée sur les 30 derniers jours de transactions.\n"
            "7. Les matrices de convergence et de divergence sont des outils d'aide à la décision complémentaires.\n"
            "8. Ce document est strictement confidentiel et destiné à l'usage professionnel uniquement.\n"
            "9. Consultez toujours un conseiller financier agréé avant toute décision d'investissement.\n"
            f"10. Rapport généré automatiquement - Version 29.0 Ultimate - {len(all_analyses)} sociétés analysées."
        )
        
        # Signature IA
        doc.add_paragraph()
        sig = doc.add_paragraph()
        sig.add_run("Analyse Multi-AI - ").italic = True
        sig.add_run(f"DeepSeek: {self.request_count['deepseek']} req | ").italic = True
        sig.add_run(f"Gemini: {self.request_count['gemini']} req | ").italic = True
        sig.add_run(f"Mistral: {self.request_count['mistral']} req").italic = True
        sig.alignment = WD_ALIGN_PARAGRAPH.CENTER
        sig.runs[0].font.size = Pt(8)
        sig.runs[0].font.color.rgb = RGBColor(128, 128, 128)
        
        # Sauvegarde
        filename = f"Rapport_Ultimate_BRVM_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(filename)
        
        logging.info(f"   ✅ Document créé: {filename}")
        
        # Préparer la synthèse pour la DB
        if market_indicators and market_indicators.get('composite'):
            synthesis_text = (
                f"Analyse ULTIMATE de {len(all_analyses)} sociétés. "
                f"Indices: BRVM Composite {market_indicators['composite']:.2f} pts. "
                f"Capitalisation: {market_indicators.get('capitalisation', 0)/1e9:.2f} Mds FCFA. "
                f"Multi-AI: DeepSeek ({self.request_count['deepseek']}), "
                f"Gemini ({self.request_count['gemini']}), Mistral ({self.request_count['mistral']})."
            )
        else:
            synthesis_text = f"Analyse ULTIMATE de {len(all_analyses)} sociétés de la BRVM."
        
        self._save_to_database(
            datetime.now().date(),
            synthesis_text,
            top_10_list,
            flop_10_list,
            market_events,
            all_company_data,
            filename
        )
        
        return filename

    def generate_all_reports(self, new_fundamental_analyses):
        """Génération du rapport ULTIMATE complet avec toutes les analyses"""
        logging.info("="*80)
        logging.info("📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V29.0 ULTIMATE)")
        logging.info("🤖 Multi-AI: DeepSeek → Gemini → Mistral")
        logging.info("📊 Analyses: Sectorielles + Convergence + Liquidité + Divergences + Risque/Horizon")
        logging.info("="*80)
        
        # Vérifier qu'au moins une clé API est disponible
        if not any([DEEPSEEK_API_KEY, GEMINI_API_KEY, MISTRAL_API_KEY]):
            logging.error("❌ Aucune clé API configurée!")
            return
        
        available_apis = []
        if DEEPSEEK_API_KEY:
            available_apis.append("DeepSeek")
        if GEMINI_API_KEY:
            available_apis.append("Gemini")
        if MISTRAL_API_KEY:
            available_apis.append("Mistral")
        
        logging.info(f"✅ API disponibles: {', '.join(available_apis)}")
        
        df = self._get_all_data_from_db()
        
        if df.empty:
            logging.error("❌ Aucune donnée disponible")
            return
        
        predictions_df = self._get_predictions_from_db()
        
        logging.info(f"🤖 Génération de {len(df)} analyse(s) avec rotation Multi-AI...")
        
        all_analyses = {}
        all_company_data = {}
        
        for idx, row in df.iterrows():
            symbol = row['symbol']
            company_id = row['company_id']
            company_name = row.get('company_name', 'N/A')
            
            hist_df = self._get_historical_data_100days(company_id)
            
            historical_summary = "Données historiques non disponibles."
            price_evolution_100d = None
            highest_price = None
            lowest_price = None
            
            if not hist_df.empty and len(hist_df) > 1:
                prix_debut = hist_df.iloc[0]['price']
                prix_fin = hist_df.iloc[-1]['price']
                prix_max = hist_df['price'].max()
                prix_min = hist_df['price'].min()
                evolution_pct = ((prix_fin - prix_debut) / prix_debut * 100) if prix_debut > 0 else 0
                
                price_evolution_100d = evolution_pct
                highest_price = prix_max
                lowest_price = prix_min
                
                historical_summary = (
                    f"Sur les 100 derniers jours, le cours a évolué de {evolution_pct:.2f}%, "
                    f"passant de {prix_debut:.0f} FCFA à {prix_fin:.0f} FCFA. "
                    f"Le cours le plus haut atteint est de {prix_max:.0f} FCFA et le plus bas de {prix_min:.0f} FCFA. "
                    f"Volume moyen échangé: {hist_df['volume'].mean():.0f} titres."
                )
            
            fundamental_text = ""
            if row.get('fundamental_summaries') and pd.notna(row['fundamental_summaries']):
                reports = row['fundamental_summaries'].split('###REPORT###')
                fundamental_parts = []
                for report in reports[:3]:
                    if report.strip():
                        parts = report.split('|||')
                        if len(parts) == 3:
                            title, date, summary = parts
                            fundamental_parts.append(f"- {title} ({date}):\n{summary}")
                
                if fundamental_parts:
                    fundamental_text = "\n\n".join(fundamental_parts)
            
            data_dict = {
                'price': row.get('price'),
                'volume': row.get('volume'),
                'historical_summary': historical_summary,
                'mm_20': row.get('mm20'),
                'mm_50': row.get('mm50'),
                'mm_decision': row.get('mm_decision'),
                'bollinger_upper': row.get('bollinger_superior'),
                'bollinger_lower': row.get('bollinger_inferior'),
                'bollinger_decision': row.get('bollinger_decision'),
                'macd_value': row.get('macd_line'),
                'macd_signal': row.get('signal_line'),
                'macd_decision': row.get('macd_decision'),
                'rsi_value': row.get('rsi'),
                'rsi_decision': row.get('rsi_decision'),
                'stochastic_k': row.get('stochastic_k'),
                'stochastic_d': row.get('stochastic_d'),
                'stochastic_decision': row.get('stochastic_decision'),
                'fundamental_analyses': fundamental_text if fundamental_text else "Aucune analyse fondamentale récente disponible.",
                'predictions': []
            }
            
            symbol_predictions = predictions_df[predictions_df['symbol'] == symbol]
            if not symbol_predictions.empty:
                # Filtrer les prédictions avec des prix valides (non NULL)
                data_dict['predictions'] = [
                    {'date': row['prediction_date'], 'price': row['predicted_price']}
                    for _, row in symbol_predictions.head(10).iterrows()
                    if pd.notna(row['predicted_price'])
                ]
                # Générer le texte seulement si des prédictions valides existent
                if data_dict['predictions']:
                    pred_list = [
                        f"{p['date']}: {p['price']:.0f} FCFA" 
                        for p in data_dict['predictions'][:5]
                        if p['price'] is not None
                    ]
                    data_dict['predictions_text'] = ", ".join(pred_list) if pred_list else "Aucune prédiction disponible"
                else:
                    data_dict['predictions_text'] = "Aucune prédiction disponible"
            else:
                data_dict['predictions_text'] = "Aucune prédiction disponible"
            
            analysis = self._generate_professional_analysis(symbol, data_dict)
            all_analyses[symbol] = analysis
            
            recommendation, rec_score = self._extract_recommendation_from_analysis(analysis)
            
            all_company_data[symbol] = {
                'company_id': company_id,
                'company_name': company_name,
                'sector': row.get('sector'),
                'current_price': float(row.get('price', 0)) if pd.notna(row.get('price')) else None,
                'price_evolution_100d': price_evolution_100d,
                'highest_price_100d': highest_price,
                'lowest_price_100d': lowest_price,
                'mm20': float(row.get('mm20', 0)) if pd.notna(row.get('mm20')) else None,
                'mm50': float(row.get('mm50', 0)) if pd.notna(row.get('mm50')) else None,
                'mm_decision': row.get('mm_decision'),
                'bollinger_upper': float(row.get('bollinger_superior', 0)) if pd.notna(row.get('bollinger_superior')) else None,
                'bollinger_lower': float(row.get('bollinger_inferior', 0)) if pd.notna(row.get('bollinger_inferior')) else None,
                'bollinger_decision': row.get('bollinger_decision'),
                'macd_value': float(row.get('macd_line', 0)) if pd.notna(row.get('macd_line')) else None,
                'macd_signal': float(row.get('signal_line', 0)) if pd.notna(row.get('signal_line')) else None,
                'macd_decision': row.get('macd_decision'),
                'rsi_value': float(row.get('rsi', 0)) if pd.notna(row.get('rsi')) else None,
                'rsi_decision': row.get('rsi_decision'),
                'stochastic_k': float(row.get('stochastic_k', 0)) if pd.notna(row.get('stochastic_k')) else None,
                'stochastic_d': float(row.get('stochastic_d', 0)) if pd.notna(row.get('stochastic_d')) else None,
                'stochastic_decision': row.get('stochastic_decision'),
                'full_analysis': analysis,
                'technical_conclusion': '',
                'fundamental_analysis': fundamental_text,
                'investment_conclusion': '',
                'recommendation': recommendation,
                'recommendation_score': rec_score,
                'confidence_level': 'Moyen',
                'risk_level': 'Moyen',
                'investment_horizon': 'Moyen terme'
            }
        
        filename = self._create_word_document(all_analyses, all_company_data)
        
        logging.info(f"\n✅ Rapport ULTIMATE généré: {filename}")
        logging.info(f"📊 Statistiques requêtes Multi-AI:")
        logging.info(f"   - DeepSeek: {self.request_count['deepseek']}")
        logging.info(f"   - Gemini: {self.request_count['gemini']}")
        logging.info(f"   - Mistral: {self.request_count['mistral']}")
        logging.info(f"   - TOTAL: {self.request_count['total']}")
        logging.info(f"\n📋 Analyses incluses:")
        logging.info(f"   ✅ Analyse par secteur")
        logging.info(f"   ✅ Matrice de convergence des signaux")
        logging.info(f"   ✅ Analyse de liquidité")
        logging.info(f"   ✅ Top 10 divergences majeures")
        logging.info(f"   ✅ Matrice Risque vs Horizon")

    def __del__(self):
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()


if __name__ == "__main__":
    try:
        report_generator = BRVMReportGenerator()
        report_generator.generate_all_reports([])
    except Exception as e:
        logging.critical(f"❌ Erreur: {e}", exc_info=True)
