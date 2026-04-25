# ==============================================================================
# MODULE: REPORT GENERATOR V30.0 FINAL - HISTORISATION + RISQUE CHIFFRÉ
# Toutes fonctionnalités V29 + Historisation DB + Calcul risque + Décisions
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
import io
import base64
try:
    import matplotlib
    matplotlib.use('Agg')          # backend non-interactif, safe en CI/GitHub Actions
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.gridspec import GridSpec
    MATPLOTLIB_OK = True
except ImportError:
    MATPLOTLIB_OK = False
    logging.warning("⚠️  matplotlib non disponible — graphiques désactivés")

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
        """Récupère les derniers indicateurs du marché + historique 100j pour commentaire"""
        # Dernière ligne valide (brvm_composite non null), ordonnée par id décroissant
        query = """
        SELECT 
            id,
            brvm_composite, 
            brvm_30, 
            brvm_prestige, 
            capitalisation_globale,
            extraction_date
        FROM new_market_indicators
        WHERE brvm_composite IS NOT NULL
          AND brvm_composite > 0
        ORDER BY id DESC
        LIMIT 1;
        """
        
        try:
            df = pd.read_sql(query, self.db_conn)
            if not df.empty:
                row = df.iloc[0]
                composite = row.get('brvm_composite')
                if pd.notna(composite):
                    indicators = {
                        'composite': float(composite),
                        'capitalisation': float(row.get('capitalisation_globale')) if pd.notna(row.get('capitalisation_globale')) else None
                    }
                    
                    # Variation journalière — comparaison avec la veille valide (id précédent)
                    try:
                        current_id = int(row.get('id'))
                        query_prev = """
                        SELECT brvm_composite
                        FROM new_market_indicators 
                        WHERE brvm_composite IS NOT NULL
                          AND brvm_composite > 0
                          AND id < %s
                        ORDER BY id DESC 
                        LIMIT 1;
                        """
                        df_prev = pd.read_sql(query_prev, self.db_conn, params=(current_id,))
                        
                        if not df_prev.empty and pd.notna(df_prev.iloc[0]['brvm_composite']):
                            prev_composite = float(df_prev.iloc[0]['brvm_composite'])
                            current_composite = float(composite)
                            var_day = ((current_composite - prev_composite) / prev_composite) * 100
                            indicators['composite_var_day'] = round(var_day, 2)
                        else:
                            indicators['composite_var_day'] = None
                    except Exception as e:
                        logging.warning(f"⚠️ Calcul variation journalière échoué: {e}")
                        indicators['composite_var_day'] = None
                    
                    # Historique 100 derniers jours valides, ordonné par id
                    try:
                        query_hist = """
                        SELECT id, brvm_composite, capitalisation_globale, extraction_date
                        FROM new_market_indicators
                        WHERE brvm_composite IS NOT NULL
                          AND brvm_composite > 0
                        ORDER BY id DESC
                        LIMIT 100;
                        """
                        df_hist = pd.read_sql(query_hist, self.db_conn)
                        if not df_hist.empty:
                            # Remettre dans l'ordre chronologique (id croissant = du plus ancien au plus récent)
                            df_hist = df_hist.sort_values('id').reset_index(drop=True)
                            indicators['history_100d'] = df_hist
                            logging.info(f"   📊 Historique BRVM: {len(df_hist)} jours valides chargés "
                                        f"({df_hist.iloc[0]['extraction_date']} → {df_hist.iloc[-1]['extraction_date']})")
                        else:
                            indicators['history_100d'] = None
                    except Exception as e:
                        logging.warning(f"⚠️ Récupération historique 100j échouée: {e}")
                        indicators['history_100d'] = None
                    
                    return indicators
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
        """
        ✅ V30.2: Récupération des données en 4 requêtes séparées + fusion Python
        Garantit que TOUTES les analyses fondamentales remontent, indépendamment
        de la présence de données de marché récentes.
        """
        logging.info("📂 Récupération des données...")
        
        # 1. Récupérer toutes les sociétés
        companies_query = """
        SELECT id, symbol, name, sector
        FROM companies
        ORDER BY symbol;
        """
        companies_df = pd.read_sql(companies_query, self.db_conn)
        logging.info(f"   ✅ {len(companies_df)} société(s) trouvée(s)")
        
        # 2. Récupérer les dernières données historiques (30 derniers jours)
        date_limite = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        hist_query = f"""
        WITH ranked_data AS (
            SELECT 
                company_id,
                id as historical_data_id,
                trade_date,
                price,
                volume,
                ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
            FROM historical_data
            WHERE trade_date >= '{date_limite}'
        )
        SELECT 
            company_id,
            historical_data_id,
            trade_date,
            price,
            volume
        FROM ranked_data
        WHERE rn = 1;
        """
        hist_df = pd.read_sql(hist_query, self.db_conn)
        logging.info(f"   ✅ {len(hist_df)} société(s) avec données historiques récentes")
        
        # 3. Récupérer les analyses techniques
        tech_query = """
        SELECT 
            historical_data_id,
            mm20, mm50, mm_decision,
            bollinger_superior, bollinger_inferior, bollinger_decision,
            macd_line, signal_line, macd_decision,
            rsi, rsi_decision,
            stochastic_k, stochastic_d, stochastic_decision
        FROM technical_analysis;
        """
        tech_df = pd.read_sql(tech_query, self.db_conn)
        logging.info(f"   ✅ {len(tech_df)} enregistrements techniques")
        
        # 4. ✅ Récupérer TOUTES les analyses fondamentales (sans filtre de date)
        fund_query = """
        SELECT 
            company_id,
            report_title,
            report_date,
            analysis_summary
        FROM fundamental_analysis
        WHERE analysis_summary IS NOT NULL
          AND analysis_summary <> ''
        ORDER BY company_id, report_date DESC NULLS LAST;
        """
        fund_df = pd.read_sql(fund_query, self.db_conn)
        logging.info(f"   ✅ {len(fund_df)} analyses fondamentales trouvées au total")
        
        if not fund_df.empty:
            fund_counts = fund_df.groupby('company_id').size().to_dict()
            companies_with_fund = len(fund_counts)
            logging.info(f"   📊 {companies_with_fund} société(s) ont des analyses fondamentales")
            for cid, count in fund_counts.items():
                sym = companies_df[companies_df['id'] == cid]['symbol'].values
                if len(sym) > 0:
                    logging.info(f"      - {sym[0]}: {count} analyse(s)")
        
        # ── Fusion Python ─────────────────────────────────────────────────────────
        result_rows = []
        
        for _, company in companies_df.iterrows():
            company_id   = company['id']
            symbol       = company['symbol']
            company_name = company['name']
            sector       = company['sector']
            
            # Données historiques
            hist_data = hist_df[hist_df['company_id'] == company_id]
            if not hist_data.empty:
                hist_row          = hist_data.iloc[0]
                historical_data_id = hist_row['historical_data_id']
                trade_date        = hist_row['trade_date']
                price             = hist_row['price']
                volume            = hist_row['volume']
                
                # Données techniques
                tech_data = tech_df[tech_df['historical_data_id'] == historical_data_id]
                if not tech_data.empty:
                    tech_row            = tech_data.iloc[0]
                    mm20                = tech_row['mm20']
                    mm50                = tech_row['mm50']
                    mm_decision         = tech_row['mm_decision']
                    bollinger_superior  = tech_row['bollinger_superior']
                    bollinger_inferior  = tech_row['bollinger_inferior']
                    bollinger_decision  = tech_row['bollinger_decision']
                    macd_line           = tech_row['macd_line']
                    signal_line         = tech_row['signal_line']
                    macd_decision       = tech_row['macd_decision']
                    rsi                 = tech_row['rsi']
                    rsi_decision        = tech_row['rsi_decision']
                    stochastic_k        = tech_row['stochastic_k']
                    stochastic_d        = tech_row['stochastic_d']
                    stochastic_decision = tech_row['stochastic_decision']
                else:
                    mm20 = mm50 = mm_decision = None
                    bollinger_superior = bollinger_inferior = bollinger_decision = None
                    macd_line = signal_line = macd_decision = None
                    rsi = rsi_decision = None
                    stochastic_k = stochastic_d = stochastic_decision = None
            else:
                historical_data_id = trade_date = price = volume = None
                mm20 = mm50 = mm_decision = None
                bollinger_superior = bollinger_inferior = bollinger_decision = None
                macd_line = signal_line = macd_decision = None
                rsi = rsi_decision = None
                stochastic_k = stochastic_d = stochastic_decision = None
            
            # ✅ Construire fundamental_summaries avec séparateurs compatibles avec le parsing existant
            company_fund_data = fund_df[fund_df['company_id'] == company_id]
            fundamental_summaries     = None
            nb_rapports_fondamentaux  = 0
            
            if not company_fund_data.empty:
                parts_list = []
                for _, fund_row in company_fund_data.iterrows():
                    title   = fund_row['report_title'] or 'Sans titre'
                    date    = fund_row['report_date']
                    date_s  = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') and date else 'Date inconnue'
                    summary = fund_row['analysis_summary'] or ''
                    if summary:
                        parts_list.append(f"{title}###SEP_FIELD###{date_s}###SEP_FIELD###{summary}")
                
                if parts_list:
                    fundamental_summaries    = '###SEP_REPORT###'.join(parts_list)
                    nb_rapports_fondamentaux = len(parts_list)
                    logging.info(f"   📄 {symbol}: {nb_rapports_fondamentaux} rapport(s) fondamental/aux chargé(s)")
            
            result_rows.append({
                'company_id':              company_id,
                'symbol':                  symbol,
                'company_name':            company_name,
                'sector':                  sector,
                'trade_date':              trade_date,
                'price':                   price,
                'volume':                  volume,
                'mm20':                    mm20,
                'mm50':                    mm50,
                'mm_decision':             mm_decision,
                'bollinger_superior':      bollinger_superior,
                'bollinger_inferior':      bollinger_inferior,
                'bollinger_decision':      bollinger_decision,
                'macd_line':               macd_line,
                'signal_line':             signal_line,
                'macd_decision':           macd_decision,
                'rsi':                     rsi,
                'rsi_decision':            rsi_decision,
                'stochastic_k':            stochastic_k,
                'stochastic_d':            stochastic_d,
                'stochastic_decision':     stochastic_decision,
                'fundamental_summaries':   fundamental_summaries,
                'nb_rapports_fondamentaux': nb_rapports_fondamentaux,
            })
        
        result_df = pd.DataFrame(result_rows)
        
        # Statistiques finales
        has_fundamental = result_df['fundamental_summaries'].notna().sum()
        logging.info(f"   ✅ Fusion terminée: {len(result_df)} société(s)")
        logging.info(f"   📊 {has_fundamental}/{len(result_df)} ont des analyses fondamentales dans le résultat final")
        
        if has_fundamental > 0:
            syms_with_fund = result_df[result_df['fundamental_summaries'].notna()]['symbol'].tolist()
            logging.info(f"   📋 Sociétés avec analyses: {', '.join(syms_with_fund)}")
        
        return result_df

    def _get_predictions_from_db(self):
        """Récupération des prédictions avec bornes IC et niveau de confiance"""
        logging.info("🔮 Récupération des prédictions (avec IC)...")

        query = """
        WITH latest_run AS (
            SELECT company_id, MAX(run_date) AS last_run
            FROM predictions
            GROUP BY company_id
        ),
        latest_predictions AS (
            SELECT
                p.company_id,
                p.prediction_date,
                p.predicted_price,
                p.lower_bound,
                p.upper_bound,
                p.confidence_level
            FROM predictions p
            JOIN latest_run lr
              ON p.company_id = lr.company_id
             AND p.run_date   = lr.last_run
            WHERE p.prediction_date >= CURRENT_DATE
        )
        SELECT
            c.symbol,
            lp.prediction_date,
            lp.predicted_price,
            lp.lower_bound,
            lp.upper_bound,
            lp.confidence_level
        FROM companies c
        LEFT JOIN latest_predictions lp ON c.id = lp.company_id
        ORDER BY c.symbol, lp.prediction_date;
        """

        try:
            df = pd.read_sql(query, self.db_conn)
            logging.info(f"   ✅ {len(df)} prédiction(s) chargées (avec IC)")
            return df
        except Exception as e:
            logging.error(f"❌ Erreur prédictions: {e}")
            return pd.DataFrame()


    # =========================================================================
    # PHASE 1 — A : Graphique cours 100j + volumes
    # =========================================================================

    def _generate_price_chart(self, symbol, hist_df):
        """
        Génère un graphique matplotlib : courbe de cours (haut) + volumes (bas).
        Retourne un objet BytesIO prêt pour doc.add_picture(), ou None si erreur.
        """
        if not MATPLOTLIB_OK or hist_df is None or hist_df.empty or len(hist_df) < 5:
            return None
        try:
            fig = plt.figure(figsize=(9, 4))
            gs  = GridSpec(2, 1, height_ratios=[3, 1], hspace=0.05)

            ax1 = fig.add_subplot(gs[0])
            ax2 = fig.add_subplot(gs[1], sharex=ax1)

            dates  = pd.to_datetime(hist_df['trade_date'])
            prices = hist_df['price'].astype(float)
            vols   = hist_df['volume'].astype(float) if 'volume' in hist_df.columns else pd.Series([0]*len(hist_df))

            # — Courbe de prix —
            ax1.plot(dates, prices, color='#1a5276', linewidth=1.6, zorder=3)
            ax1.fill_between(dates, prices, prices.min()*0.98,
                             alpha=0.08, color='#1a5276')

            # Annotations min / max
            idx_max = prices.idxmax()
            idx_min = prices.idxmin()
            ax1.annotate(f"{prices[idx_max]:,.0f}",
                         xy=(dates[idx_max], prices[idx_max]),
                         fontsize=7, color='#27ae60', fontweight='bold',
                         xytext=(0, 6), textcoords='offset points', ha='center')
            ax1.annotate(f"{prices[idx_min]:,.0f}",
                         xy=(dates[idx_min], prices[idx_min]),
                         fontsize=7, color='#c0392b', fontweight='bold',
                         xytext=(0, -12), textcoords='offset points', ha='center')

            evol = ((prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0] * 100) if prices.iloc[0] else 0
            color_title = '#27ae60' if evol >= 0 else '#c0392b'
            sign = '+' if evol >= 0 else ''
            ax1.set_title(f"{symbol} — Cours 100 derniers jours  ({sign}{evol:.1f}%)",
                          fontsize=10, fontweight='bold', color=color_title, pad=6)
            ax1.set_ylabel("Prix (FCFA)", fontsize=8)
            ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
            ax1.grid(True, linestyle='--', alpha=0.4, color='#aaaaaa')
            ax1.tick_params(axis='both', labelsize=7)
            plt.setp(ax1.get_xticklabels(), visible=False)
            ax1.spines[['top','right']].set_visible(False)

            # — Barres de volumes —
            bar_colors = ['#27ae60' if p >= prices.iloc[max(0,i-1)] else '#c0392b'
                          for i, p in enumerate(prices)]
            ax2.bar(dates, vols, color=bar_colors, alpha=0.65, width=0.8)
            ax2.set_ylabel("Volume", fontsize=7)
            ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"))
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
            ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
            plt.setp(ax2.get_xticklabels(), rotation=30, fontsize=7)
            ax2.grid(True, linestyle='--', alpha=0.3, color='#aaaaaa', axis='y')
            ax2.spines[['top','right']].set_visible(False)
            ax2.set_xlabel("Date", fontsize=8)

            fig.patch.set_facecolor('white')
            plt.tight_layout(pad=0.5)

            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=130, bbox_inches='tight',
                        facecolor='white')
            buf.seek(0)
            plt.close(fig)
            return buf
        except Exception as e:
            logging.warning(f"⚠️  Graphique {symbol}: {e}")
            try:
                plt.close('all')
            except Exception:
                pass
            return None

    # =========================================================================
    # PHASE 1 — C : Score composite d'investissement 0–100
    # =========================================================================


    # =========================================================================
    # PHASE 3 — I : Portefeuilles modèles
    # =========================================================================

    def _build_model_portfolios(self, all_company_data):
        """
        Construit 3 portefeuilles modèles à partir des scores, risques et liquidités.

        Défensif  : score ≥ 45 + risque Faible + liquidité Bonne/Excellente
        Équilibré : score ≥ 50 + risque Faible ou Moyen
        Offensif  : score ≥ 60 (tous risques) — les meilleures opportunités
        """
        defensif   = []
        equilibre  = []
        offensif   = []

        for sym, d in all_company_data.items():
            sc  = d.get('investment_score', 0)
            rl  = str(d.get('risk_level','')).lower()
            rec = str(d.get('recommendation','')).upper()
            if 'VENTE' in rec:
                continue     # aucun portefeuille ne prend un signal vente

            try:
                rd = json.loads(d.get('risk_details','{}')) if isinstance(d.get('risk_details'), str) else (d.get('risk_details') or {})
            except Exception:
                rd = {}
            liq_str = str(rd.get('liquidite','')).lower()
            liq_ok  = any(k in liq_str for k in ['excellente','bonne'])

            name  = d.get('company_name','')
            price = d.get('current_price') or 0
            entry = {'sym': sym, 'name': name, 'price': price, 'score': sc,
                     'risk': d.get('risk_level','—'), 'rec': rec}

            if sc >= 45 and 'faible' in rl and liq_ok:
                defensif.append(entry)
            if sc >= 50 and rl in ('faible','moyen'):
                equilibre.append(entry)
            if sc >= 60:
                offensif.append(entry)

        # Trier par score décroissant, limiter à 8 titres par portefeuille
        for lst in (defensif, equilibre, offensif):
            lst.sort(key=lambda x: x['score'], reverse=True)

        # Pondérations équipondérées (sauf cash 15 % dans défensif)
        def _weights(lst, cash_pct=0):
            n = min(len(lst), 8)
            if n == 0: return []
            w = round((100 - cash_pct) / n, 1)
            return [(e, w) for e in lst[:n]]

        return {
            'defensif':  (_weights(defensif, cash_pct=15),  15),
            'equilibre': (_weights(equilibre, cash_pct=10), 10),
            'offensif':  (_weights(offensif,  cash_pct=5),   5),
        }

    def _compute_investment_score(self, company_data):
        """
        Score composite 0–100 :
          Technique    30 %  (signaux achat/vente)
          Fondamental  40 %  (présence et qualité des données fondamentales)
          Risque       20 %  (risk_score inversé)
          Liquidité    10 %  (volume moyen)

        Retourne un entier 0–100 et un label (Excellent/Bon/Moyen/Faible).
        """
        score = 0.0

        # — Technique (30 %) —
        tech_keys = ['mm_decision','bollinger_decision','macd_decision',
                     'rsi_decision','stochastic_decision']
        signals = [str(company_data.get(k,'')).upper() for k in tech_keys
                   if company_data.get(k)]
        n = len(signals) or 1
        buy  = sum(1 for s in signals if 'ACHAT' in s)
        sell = sum(1 for s in signals if 'VENTE' in s)
        tech_pct = buy / n                  # 0 → 1
        score += tech_pct * 30

        # — Fondamental (40 %) —
        fund_text = company_data.get('fundamental_analysis','')
        if fund_text and len(fund_text) > 200:
            rec = str(company_data.get('recommendation','')).upper()
            if   'ACHAT FORT' in rec: fund_pts = 40
            elif 'ACHAT'      in rec: fund_pts = 32
            elif 'CONSERVER'  in rec: fund_pts = 20
            elif 'VENTE'      in rec: fund_pts = 8
            else:                     fund_pts = 16
            # Bonus si brvm_rapports disponibles
            if company_data.get('brvm_rapports_raw'):
                fund_pts = min(40, fund_pts + 5)
        else:
            fund_pts = 10   # pénalité données absentes
        score += fund_pts

        # — Risque (20 %) — risk_score est déjà 0–100 (haut = risqué)
        risk_raw = float(company_data.get('risk_score', 50))
        score += (1 - risk_raw / 100) * 20

        # — Liquidité (10 %) —
        risk_details_raw = company_data.get('risk_details','{}')
        try:
            rd = json.loads(risk_details_raw) if isinstance(risk_details_raw, str) else risk_details_raw
            liq_str = str(rd.get('liquidite','')).lower()
            if   'excellente' in liq_str: liq_pts = 10
            elif 'bonne'      in liq_str: liq_pts = 7
            elif 'faible'     in liq_str: liq_pts = 2
            else:                         liq_pts = 5
        except Exception:
            liq_pts = 5
        score += liq_pts

        score = max(0, min(100, round(score)))

        if   score >= 70: label = 'Excellent'
        elif score >= 55: label = 'Bon'
        elif score >= 40: label = 'Moyen'
        else:             label = 'Faible'

        return score, label

    # =========================================================================
    # PHASE 1 — D : Résumé Exécutif automatique
    # =========================================================================

    def _build_executive_summary(self, all_company_data, market_indicators):
        """
        Génère les données du résumé exécutif depuis les données agrégées.
        Retourne un dict avec toutes les stats nécessaires.
        """
        total   = len(all_company_data)
        achats  = sum(1 for d in all_company_data.values() if 'ACHAT' in str(d.get('recommendation','')).upper())
        ventes  = sum(1 for d in all_company_data.values() if 'VENTE' in str(d.get('recommendation','')).upper())
        neutres = total - achats - ventes

        # Meilleure société par score composite
        scored = [(sym, d.get('investment_score', 0)) for sym, d in all_company_data.items()]
        scored.sort(key=lambda x: x[1], reverse=True)
        top_sym    = scored[0][0]  if scored else '—'
        top_score  = scored[0][1]  if scored else 0

        # Secteur le plus performant
        sec_perf = {}
        for d in all_company_data.values():
            sec = d.get('sector','Autre') or 'Autre'
            p   = d.get('price_evolution_100d') or 0
            sec_perf.setdefault(sec, []).append(p)
        sec_avg = {s: sum(v)/len(v) for s,v in sec_perf.items() if v}
        best_sector  = max(sec_avg, key=sec_avg.get) if sec_avg else '—'
        best_sec_pct = sec_avg.get(best_sector, 0)
        worst_sector = min(sec_avg, key=sec_avg.get) if sec_avg else '—'

        # Titres faible liquidité
        low_liq = sum(
            1 for d in all_company_data.values()
            if 'faible' in str(d.get('risk_details','{}')).lower()
            and 'liquidite' in str(d.get('risk_details','{}')).lower()
        )

        # Marché haussier / neutre / baissier
        composite = market_indicators.get('composite') if market_indicators else None
        var_day    = market_indicators.get('composite_var_day') if market_indicators else None
        if   var_day and var_day >  1.0:  marche = 'HAUSSIER 📈'
        elif var_day and var_day < -1.0:  marche = 'BAISSIER 📉'
        else:                              marche = 'NEUTRE / STABLE ➡️'

        # Divergences majeures (tech≠fond)
        divergences = sum(
            1 for d in all_company_data.values()
            if d.get('technical_decision','') != d.get('fundamental_decision','')
            and d.get('technical_decision','') not in ('','NEUTRE')
        )

        return {
            'total': total, 'achats': achats, 'ventes': ventes, 'neutres': neutres,
            'top_sym': top_sym, 'top_score': top_score,
            'best_sector': best_sector, 'best_sec_pct': best_sec_pct,
            'worst_sector': worst_sector,
            'low_liq': low_liq, 'marche': marche,
            'divergences': divergences,
            'composite': composite, 'var_day': var_day,
        }

    # =========================================================================
    # PHASE 1 — E : Faits marquants depuis google_alerts_rapports
    # =========================================================================

    def _get_google_alerts_events(self):
        """
        Récupère les alertes depuis google_alerts_rapports.
        Exploite toutes les colonnes disponibles : mail_subject, alert_keyword,
        points_cles, mot_cle, rapport_type, pertinence, sentiment.
        Fallback sur new_market_events si vide.
        """
        logging.info("📰 Récupération google_alerts_rapports (enrichi)...")
        query = """
        SELECT
            mail_date,
            mail_subject,
            titre,
            resume,
            points_cles,
            sentiment,
            pertinence,
            categorie,
            rapport_type,
            alert_keyword,
            mot_cle,
            source_url
        FROM google_alerts_rapports
        WHERE (resume IS NOT NULL AND resume <> '')
           OR (mail_subject IS NOT NULL AND mail_subject <> '')
        ORDER BY mail_date DESC NULLS LAST
        LIMIT 20;
        """
        try:
            df = pd.read_sql(query, self.db_conn)
            if not df.empty:
                logging.info(f"   ✅ {len(df)} alerte(s) google chargée(s)")
                return df
        except Exception as e:
            logging.warning(f"⚠️  google_alerts_rapports non disponible: {e}")

        # Fallback : new_market_events
        logging.info("   ↩️  Fallback → new_market_events")
        try:
            df2 = pd.read_sql(
                "SELECT event_date AS mail_date, event_summary AS resume "
                "FROM new_market_events ORDER BY event_date DESC LIMIT 10;",
                self.db_conn
            )
            return df2
        except Exception:
            return pd.DataFrame()

    def _get_brvm_actualites(self):
        """
        Charge les documents officiels récents (AG, dividendes, convocations,
        résultats) depuis brvm_documents — toutes sociétés confondues.
        Retourne un DataFrame trié par date décroissante.
        """
        logging.info("📄 Chargement actualités brvm_documents (AG/dividendes/convocations)...")
        query = """
        SELECT
            societe_confirmee,
            titre,
            date_doc,
            date_publication,
            categorie,
            type_document,
            rapport_type,
            resume,
            points_cles,
            impact,
            doc_url
        FROM brvm_documents
        WHERE resume IS NOT NULL AND resume <> ''
        ORDER BY date_doc DESC NULLS LAST, date_publication DESC NULLS LAST
        LIMIT 30;
        """
        try:
            df = pd.read_sql(query, self.db_conn)
            logging.info(f"   ✅ {len(df)} document(s) brvm_documents chargés (vue globale)")
            return df
        except Exception as e:
            logging.warning(f"⚠️  brvm_documents actualités: {e}")
            return pd.DataFrame()

    def _get_brvm_documents(self):
        """
        Charge tous les documents de brvm_documents groupés par société confirmée.
        Retourne un dict: { symbol -> [doc, doc, ...] }
        Chaque doc = { titre, date_doc, resume, points_cles, impact, categorie, doc_url }
        """
        logging.info("📂 Chargement brvm_documents...")

        query = """
        SELECT
            societe_confirmee,
            titre,
            date_doc,
            resume,
            points_cles,
            impact,
            categorie,
            doc_url
        FROM brvm_documents
        WHERE societe_confirmee IS NOT NULL
          AND societe_confirmee <> ''
          AND resume IS NOT NULL
          AND resume <> ''
        ORDER BY societe_confirmee, date_doc DESC NULLS LAST;
        """

        try:
            df = pd.read_sql(query, self.db_conn)
            logging.info(f"   ✅ {len(df)} document(s) chargé(s) depuis brvm_documents")
        except Exception as e:
            logging.error(f"❌ Erreur brvm_documents: {e}")
            return {}

        docs_by_symbol = {}
        for _, row in df.iterrows():
            sym = str(row['societe_confirmee']).strip().upper()
            if not sym:
                continue
            doc = {
                'titre':      row.get('titre') or 'Sans titre',
                'date_doc':   str(row.get('date_doc') or 'Date inconnue'),
                'resume':     row.get('resume') or '',
                'points_cles': row.get('points_cles'),   # jsonb → peut être list ou None
                'impact':     row.get('impact') or 'neutre',
                'categorie':  row.get('categorie') or '',
                'doc_url':    row.get('doc_url') or '',
            }
            docs_by_symbol.setdefault(sym, []).append(doc)

        logging.info(f"   📊 {len(docs_by_symbol)} société(s) avec documents brvm_documents")
        return docs_by_symbol


    # =========================================================================
    # brvm_rapports_societes — annonces & communiqués officiels par société
    # =========================================================================

    def _get_brvm_rapports_societes(self):
        """
        Charge tous les rapports/communiqués depuis brvm_rapports_societes,
        groupés par société.
        Retourne un dict: { symbol_upper -> [rapport, ...] }
        Chaque rapport = {
            annee, type_rapport, doc_titre, doc_url,
            resume, points_cles, indicateurs,
            recommandation, risques, perspectives,
            date_rapport, created_at
        }
        """
        logging.info("📋 Chargement brvm_rapports_societes...")

        query = """
        SELECT
            societe,
            annee,
            type_rapport,
            doc_titre,
            doc_url,
            resume,
            points_cles,
            indicateurs,
            recommandation,
            risques,
            perspectives,
            date_rapport,
            created_at
        FROM brvm_rapports_societes
        WHERE societe IS NOT NULL
          AND societe <> ''
          AND resume IS NOT NULL
          AND resume <> ''
        ORDER BY societe, annee DESC NULLS LAST, created_at DESC NULLS LAST;
        """

        try:
            df = pd.read_sql(query, self.db_conn)
            logging.info(f"   ✅ {len(df)} entrée(s) dans brvm_rapports_societes")
        except Exception as e:
            logging.error(f"❌ Erreur brvm_rapports_societes: {e}")
            return {}

        rapports_by_symbol = {}
        for _, row in df.iterrows():
            sym = str(row['societe']).strip().upper()
            if not sym:
                continue
            rapport = {
                'annee':          row.get('annee'),
                'type_rapport':   row.get('type_rapport') or '',
                'doc_titre':      row.get('doc_titre') or 'Sans titre',
                'doc_url':        row.get('doc_url') or '',
                'resume':         row.get('resume') or '',
                'points_cles':    row.get('points_cles'),    # jsonb
                'indicateurs':    row.get('indicateurs'),    # jsonb
                'recommandation': row.get('recommandation') or '',
                'risques':        row.get('risques') or '',
                'perspectives':   row.get('perspectives') or '',
                'date_rapport':   str(row.get('date_rapport') or ''),
                'created_at':     str(row.get('created_at') or ''),
            }
            rapports_by_symbol.setdefault(sym, []).append(rapport)

        logging.info(
            f"   📊 {len(rapports_by_symbol)} société(s) avec rapports brvm_rapports_societes"
        )
        return rapports_by_symbol

    def _format_rapports_societes_for_ai(self, rapports):
        """
        Formate les entrées brvm_rapports_societes pour le prompt IA.
        Max 5 entrées, résumé tronqué à 500 chars.
        """
        if not rapports:
            return ""

        parts = []
        for i, r in enumerate(rapports[:5]):
            # Points clés
            pk = r.get('points_cles')
            if isinstance(pk, list):
                pk_text = " | ".join(str(p) for p in pk[:4])
            elif isinstance(pk, str) and pk.strip():
                pk_text = pk[:250]
            else:
                pk_text = ""

            # Indicateurs financiers (jsonb)
            ind = r.get('indicateurs')
            ind_text = ""
            if isinstance(ind, dict) and ind:
                pairs = [f"{k}: {v}" for k, v in list(ind.items())[:6]]
                ind_text = " | ".join(pairs)
            elif isinstance(ind, str) and ind.strip():
                ind_text = ind[:200]

            label = f"[RAPPORT {i+1}] {r['doc_titre']}"
            if r.get('annee'):
                label += f" ({r['annee']})"
            if r.get('type_rapport'):
                label += f" — {r['type_rapport']}"

            block = label + "\n"
            block += f"Résumé: {r['resume'][:500]}\n"
            if pk_text:
                block += f"Points clés: {pk_text}\n"
            if ind_text:
                block += f"Indicateurs: {ind_text}\n"
            if r.get('recommandation'):
                block += f"Recommandation source: {r['recommandation']}\n"
            if r.get('risques'):
                block += f"Risques identifiés: {r['risques'][:200]}\n"
            if r.get('perspectives'):
                block += f"Perspectives: {r['perspectives'][:200]}\n"
            parts.append(block)

        return "\n\n".join(parts)

    def _format_rapports_societes_for_word(self, rapport, idx):
        """Prépare un rapport brvm_rapports_societes pour l'insertion Word."""
        pk = rapport.get('points_cles')
        if isinstance(pk, list):
            points = [str(p) for p in pk[:6]]
        elif isinstance(pk, str) and pk.strip():
            try:
                import json as _j
                parsed = _j.loads(pk)
                points = [str(p) for p in parsed[:6]] if isinstance(parsed, list) else [pk[:200]]
            except Exception:
                points = [pk[:200]]
        else:
            points = []

        ind = rapport.get('indicateurs')
        indicateurs_kv = {}
        if isinstance(ind, dict):
            indicateurs_kv = {k: v for k, v in list(ind.items())[:8]}
        elif isinstance(ind, str) and ind.strip():
            try:
                import json as _j
                parsed = _j.loads(ind)
                if isinstance(parsed, dict):
                    indicateurs_kv = {k: v for k, v in list(parsed.items())[:8]}
            except Exception:
                pass

        rec = str(rapport.get('recommandation', '')).upper()
        if 'ACHAT' in rec:
            rec_color = RGBColor(0, 128, 0)
        elif 'VENTE' in rec:
            rec_color = RGBColor(192, 0, 0)
        else:
            rec_color = RGBColor(100, 100, 100)

        return {
            'idx':            idx,
            'annee':          rapport.get('annee') or '—',
            'type_rapport':   rapport.get('type_rapport') or '',
            'doc_titre':      rapport.get('doc_titre') or 'Sans titre',
            'doc_url':        rapport.get('doc_url') or '',
            'resume':         rapport.get('resume') or '',
            'points':         points,
            'indicateurs':    indicateurs_kv,
            'recommandation': rapport.get('recommandation') or '',
            'rec_color':      rec_color,
            'risques':        rapport.get('risques') or '',
            'perspectives':   rapport.get('perspectives') or '',
            'date_rapport':   rapport.get('date_rapport') or '',
        }

    def _format_brvm_documents_for_ai(self, docs):
        """
        Formate les documents brvm_documents en texte structuré pour l'IA.
        Limite à 5 documents max pour ne pas dépasser le contexte.
        """
        if not docs:
            return ""

        parts = []
        for i, doc in enumerate(docs[:5]):
            # Points clés : peut être une list Python (jsonb décodé par psycopg2)
            pk = doc.get('points_cles')
            if isinstance(pk, list):
                pk_text = " | ".join(str(p) for p in pk[:5]) if pk else ""
            elif isinstance(pk, str):
                pk_text = pk[:300]
            else:
                pk_text = ""

            impact_icon = {"positif": "🟢", "negatif": "🔴", "neutre": "⚪"}.get(
                str(doc.get('impact', 'neutre')).lower(), "⚪"
            )

            parts.append(
                f"[DOC {i+1}] {doc['titre']} | {doc['date_doc']} | "
                f"Catégorie: {doc['categorie']} | Impact: {impact_icon} {doc['impact']}\n"
                f"Résumé: {doc['resume'][:600]}\n"
                + (f"Points clés: {pk_text}\n" if pk_text else "")
            )

        return "\n\n".join(parts)

    def _format_brvm_documents_for_word(self, doc, doc_index):
        """
        Retourne un dict structuré prêt pour l'insertion Word.
        """
        pk = doc.get('points_cles')
        if isinstance(pk, list):
            points = [str(p) for p in pk[:6]]
        elif isinstance(pk, str) and pk.strip():
            # Essai de parse JSON si c'est une chaîne
            try:
                import json as _json
                parsed = _json.loads(pk)
                points = [str(p) for p in parsed[:6]] if isinstance(parsed, list) else [pk[:200]]
            except Exception:
                points = [pk[:200]]
        else:
            points = []

        impact = str(doc.get('impact', 'neutre')).lower()
        impact_icon = {"positif": "🟢 POSITIF", "negatif": "🔴 NÉGATIF", "neutre": "⚪ NEUTRE"}.get(impact, "⚪ NEUTRE")
        impact_color = {"positif": RGBColor(0, 128, 0), "negatif": RGBColor(192, 0, 0), "neutre": RGBColor(100, 100, 100)}.get(impact, RGBColor(100, 100, 100))

        # Badge fraîcheur pour brvm_documents
        date_doc_raw = str(doc.get('date_doc','') or '')
        try:
            from datetime import date as _d2
            import re as _re2
            m = _re2.search(r'(\d{4})[-/](\d{1,2})', date_doc_raw)
            if m:
                dy, dm = int(m.group(1)), int(m.group(2))
                td2    = _d2.today()
                mago2  = (td2.year - dy)*12 + (td2.month - dm)
                fresh2 = "⭐ RÉCENT" if mago2 <= 3 else ("📅 À JOUR" if mago2 <= 12 else "⚠️ ANCIEN")
                date_doc_display = f"{date_doc_raw} {fresh2}"
            else:
                date_doc_display = date_doc_raw
        except Exception:
            date_doc_display = date_doc_raw

        return {
            'num':          doc_index,
            'titre':        doc.get('titre', 'Sans titre'),
            'date_doc':     date_doc_display,
            'categorie':    doc.get('categorie', ''),
            'resume':       doc.get('resume', ''),
            'points':       points,
            'impact':       impact_icon,
            'impact_color': impact_color,
            'doc_url':      doc.get('doc_url', ''),
        }

    def _extract_recommendation_from_analysis(self, analysis_text,
                                                tech_decision=None,
                                                fund_decision=None):
        """
        Extrait et CORRIGE la recommandation du texte d'analyse IA.

        Règle de cohérence :
          • Si tech=VENTE et fond=ACHAT (ou inverse) → ajuste à NEUTRE/CONSERVER
            avec note de divergence, sauf si l'IA a explicitement justifié
            le choix final avec les mots 'malgré' ou 'divergence'.
          • Si tech et fond convergent → confirme la recommandation IA.
          • Sinon → recommandation IA brute.
        """
        al = analysis_text.lower()

        # 1. Lire la recommandation brute de l'IA
        if 'achat fort' in al:
            raw_rec, raw_score = 'ACHAT FORT', 5
        elif 'achat' in al:
            raw_rec, raw_score = 'ACHAT', 4
        elif 'conserver' in al:
            raw_rec, raw_score = 'CONSERVER', 3
        elif 'vente forte' in al:
            raw_rec, raw_score = 'VENTE FORTE', 1
        elif 'vente' in al:
            raw_rec, raw_score = 'VENTE', 2
        else:
            raw_rec, raw_score = 'CONSERVER', 3

        # 2. Correction de cohérence tech / fondamental
        if tech_decision and fund_decision:
            td = str(tech_decision).upper()
            fd = str(fund_decision).upper()
            divergence_forte = (
                ('ACHAT' in td and 'VENTE' in fd) or
                ('VENTE' in td and 'ACHAT' in fd)
            )
            # L'IA a-t-elle elle-même reconnu la divergence ?
            ia_acknowledged = any(kw in al for kw in
                                  ['malgré', 'divergence', 'contradiction',
                                   'incohérence', 'signal mixte'])

            if divergence_forte and not ia_acknowledged:
                # Recommandation ajustée
                if raw_rec in ('ACHAT FORT', 'ACHAT'):
                    return 'CONSERVER ⚠️', 3   # score 3 = neutre
                elif raw_rec in ('VENTE FORTE', 'VENTE'):
                    return 'CONSERVER ⚠️', 3
                # else: déjà CONSERVER, on laisse

        return raw_rec, raw_score

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
        """
        Génération analyse professionnelle avec rotation Multi-AI.
        ✅ V30: instruction fondamentale dynamique + vérification post-génération
        """
        
        if attempt > 1:
            logging.info(f"    🔄 {symbol}: Tentative {attempt}/{max_attempts}")
        
        # ── Préparer l'instruction fondamentale selon la disponibilité des données ──
        fundamental_text = data_dict.get('fundamental_analyses', '')
        has_fundamental = (
            fundamental_text
            and fundamental_text.strip()
            and "Aucun rapport" not in fundamental_text
        )
        
        if has_fundamental:
            nb_rapports = fundamental_text.count('--- RAPPORT:')
            if nb_rapports == 0 and fundamental_text.strip():
                nb_rapports = 1
            preview = fundamental_text[:300].replace('\n', ' ') + "..."
            logging.info(f"    📊 {symbol}: {len(fundamental_text)} caractères d'analyses fondamentales ({nb_rapports} rapport(s))")
            logging.info(f"    📋 Extrait: {preview}")
            
            instruction_fondamentale = f"""
⚠️ INSTRUCTION IMPÉRATIVE — ANALYSES FONDAMENTALES DISPONIBLES:
{len(fundamental_text)} caractères de données financières officielles sont fournis ci-dessous ({nb_rapports} rapport(s)).

TU DOIS OBLIGATOIREMENT:
1. UTILISER CES DONNÉES dans la Partie 3 — c'est une instruction ABSOLUE, non optionnelle
2. Mentionner explicitement la date de CHAQUE rapport utilisé
3. Citer les chiffres clés: chiffre d'affaires, résultat net, dividendes, ratios
4. Si plusieurs rapports, montrer l'évolution temporelle des indicateurs
5. NE JAMAIS écrire que les données sont absentes ou indisponibles — elles sont là
6. Si les rapports datent d'avant 2025, précise-le mais analyse-les quand même"""
        else:
            logging.warning(f"    ⚠️ {symbol}: Aucune analyse fondamentale en base")
            instruction_fondamentale = """
ℹ️ ABSENCE DE DONNÉES FONDAMENTALES:
Aucun rapport financier n'a été trouvé en base pour cette société.
Indique clairement cette absence dans la Partie 3 et base ta conclusion uniquement
sur les indicateurs techniques et les prédictions."""
        
        prompt = f"""Tu es un analyste financier professionnel spécialisé sur le marché de la BRVM (Bourse Régionale des Valeurs Mobilières, Afrique de l'Ouest). Analyse l'action {symbol} et génère un rapport structuré en 4 parties.

📊 DONNÉES DISPONIBLES:

**Évolution du cours (100 derniers jours):**
{data_dict.get('historical_summary', 'Données non disponibles')}

**Indicateurs techniques:**
- Moyennes Mobiles: MM20={data_dict.get('mm_20', 'N/A')}, MM50={data_dict.get('mm_50', 'N/A')}, Décision={data_dict.get('mm_decision', 'N/A')}
- Bandes de Bollinger: Borne supérieure={data_dict.get('bollinger_upper', 'N/A')}, Borne inférieure={data_dict.get('bollinger_lower', 'N/A')}, Prix actuel={data_dict.get('price', 'N/A')}, Décision={data_dict.get('bollinger_decision', 'N/A')}
- MACD: Valeur={data_dict.get('macd_value', 'N/A')}, Signal={data_dict.get('macd_signal', 'N/A')}, Décision={data_dict.get('macd_decision', 'N/A')}
- RSI: Valeur={data_dict.get('rsi_value', 'N/A')}, Décision={data_dict.get('rsi_decision', 'N/A')}
- Stochastique: %K={data_dict.get('stochastic_k', 'N/A')}, %D={data_dict.get('stochastic_d', 'N/A')}, Décision={data_dict.get('stochastic_decision', 'N/A')}

**ANALYSES FONDAMENTALES DISPONIBLES (RAPPORTS FINANCIERS OFFICIELS):**
{fundamental_text if has_fundamental else "Aucun rapport financier enregistré en base pour cette société."}

{instruction_fondamentale}

**Prédictions IA (10 prochains jours ouvrables):**
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
- **Moyennes Mobiles**: Interprète MM20 et MM50, leur position relative au cours actuel, justifie la décision
- **Bandes de Bollinger**: Explique la position du cours par rapport aux bornes, la volatilité, justifie la décision
- **MACD**: Analyse la divergence MACD-Signal, le momentum, justifie la décision
- **RSI**: Interprète la valeur (suracheté >70, survente <30, neutre 30-70), justifie la décision
- **Stochastique**: Analyse %K et %D, leur croisement éventuel, justifie la décision

Puis rédige une **conclusion technique** de 3-4 lignes synthétisant tous les indicateurs.

**PARTIE 3 : ANALYSE FONDAMENTALE (SECTION CRITIQUE)**

Rédige un paragraphe détaillé de 8-10 lignes:
- UTILISE OBLIGATOIREMENT les analyses fondamentales fournies ci-dessus si elles existent
- Pour CHAQUE rapport disponible, mentionne sa date et résume ses points clés (CA, résultat net, dividendes)
- Si plusieurs rapports, montre l'évolution temporelle des chiffres
- Donne une recommandation fondamentale basée sur CES données
- NE DIS PAS que les données sont absentes si elles sont fournies ci-dessus

**PARTIE 4 : CONCLUSION D'INVESTISSEMENT**

Rédige un paragraphe de 5-6 lignes:
- Synthétise les 3 analyses précédentes (cours, technique, fondamental)
- Donne une recommandation finale claire: **ACHAT FORT**, **ACHAT**, **CONSERVER**, **VENTE**, ou **VENTE FORTE**
- Justifie par la convergence ou divergence des signaux
- Indique le niveau de confiance: Élevé, Moyen, ou Faible
- Mentionne le niveau de risque: Faible, Moyen, ou Élevé
- Suggère un horizon d'investissement (court, moyen, long terme)

═══════════════════════════════════════════════════════════════

RAPPELS IMPÉRATIFS:
- Rédige en français professionnel avec des paragraphes fluides (pas de bullet points)
- Sois précis avec les chiffres — cite les valeurs exactes des données fournies
- Si des analyses fondamentales sont fournies, TU DOIS LES UTILISER — instruction OBLIGATOIRE
- Mentionne TOUJOURS la date des rapports fondamentaux utilisés
- Reste factuel et objectif"""
        
        # ── Rotation Multi-AI: DeepSeek → Gemini → Mistral ──────────────────────────
        analysis = None
        provider = None
        
        logging.info(f"    🤖 {symbol}: Tentative DeepSeek...")
        analysis, provider = self._generate_analysis_with_deepseek(symbol, data_dict, prompt)
        
        if not analysis:
            logging.info(f"    🤖 {symbol}: Tentative Gemini...")
            analysis, provider = self._generate_analysis_with_gemini(symbol, data_dict, prompt)
        
        if not analysis:
            logging.info(f"    🤖 {symbol}: Tentative Mistral...")
            analysis, provider = self._generate_analysis_with_mistral(symbol, data_dict, prompt)
        
        if not analysis:
            if attempt < max_attempts:
                logging.warning(f"    ⚠️  {symbol}: Toutes API échouées, retry {attempt+1}/{max_attempts}")
                time.sleep(10)
                return self._generate_professional_analysis(symbol, data_dict, attempt + 1, max_attempts)
            else:
                logging.error(f"    ❌ {symbol}: Échec définitif après {max_attempts} tentatives")
                return self._generate_fallback_analysis(symbol, data_dict)
        
        # ── Vérification post-génération: l'IA a-t-elle bien utilisé les fondamentaux ──
        if has_fundamental and "aucune donnée" in analysis.lower() and "fondamental" in analysis.lower():
            logging.warning(f"    ⚠️ {symbol}: L'IA a potentiellement ignoré les analyses fondamentales malgré les instructions")
        
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
        fundamental_text = data_dict.get('fundamental_analyses', '')
        if fundamental_text and fundamental_text.strip() and "Aucun rapport" not in fundamental_text:
            analysis += f"{fundamental_text}\n\n"
            analysis += "Ces analyses fondamentales, bien que pouvant dater de périodes antérieures, fournissent des indications précieuses sur la santé financière historique de l'entreprise.\n\n"
        else:
            analysis += "Aucune analyse fondamentale n'est disponible dans la base de données pour cette société.\n\n"
        
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

    def _calculate_risk_score(self, data):
        """
        ✅ V30: Calcul du score de risque chiffré (0-100) avec 5 critères
        
        Critères:
        - Volatilité (30%)
        - Beta vs marché (20%)  
        - Liquidité (20%)
        - Divergence signaux (15%)
        - Performance historique (15%)
        """
        import numpy as np
        
        risk_score = 0
        details = {}
        
        # 1. Volatilité (30%) - écart-type des prix / moyenne
        hist_df = self._get_historical_data_100days(data.get('company_id'))
        volatility = 0
        
        if not hist_df.empty and len(hist_df) > 1:
            prices = hist_df['price'].values
            mean_price = np.mean(prices)
            if mean_price > 0:
                volatility = np.std(prices) / mean_price
            
            if volatility < 0.05:  # < 5%
                vol_score = 10
                details['volatilite'] = f"{volatility*100:.1f}% (Faible)"
            elif volatility < 0.15:  # 5-15%
                vol_score = 30
                details['volatilite'] = f"{volatility*100:.1f}% (Moyenne)"
            else:  # > 15%
                vol_score = 60
                details['volatilite'] = f"{volatility*100:.1f}% (Élevée)"
            
            risk_score += vol_score * 0.30
        else:
            details['volatilite'] = "Données insuffisantes"
            risk_score += 30 * 0.30  # Score moyen par défaut
        
        # 2. Beta vs marché (20%) - approximation basée sur volatilité
        beta = volatility / 0.10 if volatility > 0 else 1.0
        
        if beta < 0.8:
            beta_score = 10
            details['beta'] = f"{beta:.2f} (Défensif)"
        elif beta < 1.2:
            beta_score = 20
            details['beta'] = f"{beta:.2f} (Neutre)"
        else:
            beta_score = 40
            details['beta'] = f"{beta:.2f} (Agressif)"
        
        risk_score += beta_score * 0.20
        
        # 3. Liquidité (20%) - volume moyen
        # Calculer le volume moyen depuis historical_data
        company_id = data.get('company_id')
        avg_volume = 0
        
        if company_id and not hist_df.empty:
            avg_volume = hist_df['volume'].mean() if 'volume' in hist_df.columns else 0
        
        if avg_volume > 10000:
            liq_score = 5
            details['liquidite'] = f"{avg_volume:.0f} titres/j (Excellente)"
        elif avg_volume > 1000:
            liq_score = 15
            details['liquidite'] = f"{avg_volume:.0f} titres/j (Bonne)"
        elif avg_volume > 0:
            liq_score = 40
            details['liquidite'] = f"{avg_volume:.0f} titres/j (Faible - RISQUE)"
        else:
            liq_score = 30
            details['liquidite'] = "Non calculable"
        
        risk_score += liq_score * 0.20
        
        # 4. Divergence signaux (15%)
        # Calculer le score de divergence
        tech_signals = []
        for key in ['mm_decision', 'bollinger_decision', 'macd_decision', 'rsi_decision', 'stochastic_decision']:
            val = data.get(key)
            if val:
                tech_signals.append(str(val))
        
        buy_signals = [s for s in tech_signals if 'Achat' in s]
        sell_signals = [s for s in tech_signals if 'Vente' in s]
        divergence_score = abs(len(buy_signals) - len(sell_signals))
        
        div_score = min(divergence_score * 5, 30)
        details['divergence'] = f"{divergence_score}/{len(tech_signals)} signaux"
        
        risk_score += div_score * 0.15
        
        # 5. Performance historique (15%) - stabilité des rendements
        if not hist_df.empty and len(hist_df) > 1:
            returns = hist_df['price'].pct_change().dropna()
            if len(returns) > 0:
                perf_volatility = returns.std() * 100
            else:
                perf_volatility = 10
            
            if perf_volatility < 5:
                perf_score = 5
                details['stabilite'] = f"{perf_volatility:.1f}% (Excellente)"
            elif perf_volatility < 15:
                perf_score = 15
                details['stabilite'] = f"{perf_volatility:.1f}% (Moyenne)"
            else:
                perf_score = 30
                details['stabilite'] = f"{perf_volatility:.1f}% (Instable)"
            
            risk_score += perf_score * 0.15
        else:
            details['stabilite'] = "Données insuffisantes"
            risk_score += 15 * 0.15
        
        # Classification finale
        if risk_score < 20:
            risk_level = "Faible"
        elif risk_score < 50:
            risk_level = "Moyen"
        else:
            risk_level = "Élevé"
        
        return {
            'score': round(risk_score, 2),
            'level': risk_level,
            'details': details
        }

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
                    # ✅ V30: HISTORISATION - Plus de ON CONFLICT, toujours INSERT
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
                            recommendation, confidence_level, risk_level, investment_horizon,
                            technical_decision, fundamental_decision, risk_score, risk_details
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s
                        );
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
                        company_data.get('investment_horizon'),
                        company_data.get('technical_decision', 'NEUTRE'),
                        company_data.get('fundamental_decision', 'NEUTRE'),
                        company_data.get('risk_score', 0.0),
                        company_data.get('risk_details', '{}')
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
        
        version_p = doc.add_paragraph(f"Version 30.2 - Analyses Multi-AI (DeepSeek + Gemini + Mistral)")
        version_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        version_run = version_p.runs[0]
        version_run.font.size = Pt(9)
        version_run.font.italic = True
        version_run.font.color.rgb = RGBColor(128, 128, 128)
        
        doc.add_paragraph()

        # ========== RÉSUMÉ EXÉCUTIF (Page 1) ==========
        market_indicators_pre = self._get_market_indicators()
        exec_data = self._build_executive_summary(all_company_data, market_indicators_pre)

        doc.add_paragraph()
        exec_box = doc.add_paragraph()
        exec_box.paragraph_format.space_before = Pt(6)
        exec_box.paragraph_format.space_after  = Pt(6)

        def _add_exec_line(para, label, value, val_color=None):
            r_lbl = para.add_run(f"  {label}  ")
            r_lbl.bold = True
            r_lbl.font.size = Pt(10)
            r_val = para.add_run(str(value) + "\n")
            r_val.font.size = Pt(10)
            if val_color:
                r_val.font.color.rgb = val_color

        # Encadré bleu foncé
        from docx.oxml.ns import qn as _qn
        from docx.oxml import OxmlElement as _OxmlElement

        def _shade_para(para, hex_color):
            pPr = para._p.get_or_add_pPr()
            shd = _OxmlElement('w:shd')
            shd.set(_qn('w:val'),  'clear')
            shd.set(_qn('w:color'),'auto')
            shd.set(_qn('w:fill'), hex_color)
            pPr.append(shd)

        lines = [
            ("📊 RÉSUMÉ EXÉCUTIF", "", None),
            ("─" * 55, "", None),
            ("Marché :", exec_data['marche'],
             RGBColor(0,128,0) if 'HAUSSE' in exec_data['marche'].upper()
             else RGBColor(192,0,0) if 'BAIS' in exec_data['marche'].upper()
             else RGBColor(80,80,80)),
            ("Signaux :",
             f"{exec_data['achats']} ACHAT  |  {exec_data['neutres']} NEUTRE  |  {exec_data['ventes']} VENTE  (sur {exec_data['total']} sociétés)",
             RGBColor(0,80,160)),
            ("Top opportunité :",
             f"{exec_data['top_sym']}  (score {exec_data['top_score']}/100)",
             RGBColor(0,128,0)),
            ("Secteur surperformant :",
             f"{exec_data['best_sector']}  ({exec_data['best_sec_pct']:+.1f}%)",
             RGBColor(0,128,0)),
            ("Secteur sous-performant :", exec_data['worst_sector'], RGBColor(160,60,0)),
            ("Divergences tech/fond :",
             f"{exec_data['divergences']} sociétés — vérifier avant d'investir",
             RGBColor(160,100,0)),
            ("⚠️ Liquidité réduite :",
             f"{exec_data['low_liq']} titres à faible liquidité — risque de sortie",
             RGBColor(192,0,0)),
        ]

        for label, value, color in lines:
            ep = doc.add_paragraph()
            ep.paragraph_format.left_indent  = Pt(18)
            ep.paragraph_format.right_indent = Pt(18)
            ep.paragraph_format.space_before = Pt(1)
            ep.paragraph_format.space_after  = Pt(1)
            _shade_para(ep, 'EBF5FB')
            if label.startswith('─') or label.startswith('📊'):
                r = ep.add_run(label if label.startswith('📊') else '')
                r.bold = True
                r.font.size = Pt(11)
                r.font.color.rgb = RGBColor(0,51,102)
            else:
                r_l = ep.add_run(f"{label}  ")
                r_l.bold = True
                r_l.font.size = Pt(9.5)
                r_v = ep.add_run(value)
                r_v.font.size = Pt(9.5)
                if color:
                    r_v.font.color.rgb = color

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
            
            # ✅ Capitalisation corrigée: valeur brute en FCFA → diviser par 1e9 pour obtenir des milliards
            # Exemple: 15 660 629 773 994 FCFA / 1e9 = 15 660,630 milliards FCFA
            if market_indicators.get('capitalisation'):
                cap_raw = market_indicators['capitalisation']
                cap_milliards = cap_raw / 1e9  # toujours en milliards FCFA
                # Formatage français: séparateur de milliers = espace, décimale = virgule
                cap_entier = int(cap_milliards)
                cap_decimale = round((cap_milliards - cap_entier) * 1000)
                cap_display = f"{cap_entier:,}".replace(",", " ") + f",{cap_decimale:03d}"
                intro.add_run(f"La capitalisation globale du marché atteint {cap_display} milliards FCFA.")
        else:
            intro.add_run("Les indicateurs de marché seront mis à jour prochainement.")
        
        intro.paragraph_format.space_after = Pt(12)
        doc.add_paragraph()
        
        # ✅ Commentaire évolution BRVM Composite sur 100 derniers jours
        if market_indicators and market_indicators.get('history_100d') is not None:
            df_hist = market_indicators['history_100d']
            # Les données sont déjà filtrées (brvm_composite NOT NULL > 0) et triées par id croissant
            
            if len(df_hist) >= 2:
                doc.add_heading('📊 Évolution de l\'indice BRVM Composite (100 derniers jours)', level=3)
                
                composite_first = float(df_hist.iloc[0]['brvm_composite'])
                composite_last  = float(df_hist.iloc[-1]['brvm_composite'])
                composite_max   = float(df_hist['brvm_composite'].max())
                composite_min   = float(df_hist['brvm_composite'].min())
                composite_evol  = ((composite_last - composite_first) / composite_first * 100)
                nb_jours        = len(df_hist)
                date_debut      = df_hist.iloc[0]['extraction_date']
                date_fin        = df_hist.iloc[-1]['extraction_date']
                
                p_composite = doc.add_paragraph()
                p_composite.add_run("BRVM Composite — ").bold = True
                p_composite.add_run(
                    f"Sur les {nb_jours} derniers jours de cotation "
                    f"({date_debut} au {date_fin}), l'indice BRVM Composite a évolué de "
                )
                perf_run = p_composite.add_run(f"{composite_evol:+.2f}%")
                perf_run.bold = True
                perf_run.font.color.rgb = RGBColor(0, 128, 0) if composite_evol >= 0 else RGBColor(192, 0, 0)
                p_composite.add_run(
                    f", passant de {composite_first:.2f} pts à {composite_last:.2f} pts. "
                    f"Le plus haut atteint est {composite_max:.2f} pts et le plus bas {composite_min:.2f} pts. "
                )
                if composite_evol > 2:
                    p_composite.add_run("L'indice affiche une tendance haussière sur la période, témoignant d'un regain de confiance des investisseurs.")
                elif composite_evol < -2:
                    p_composite.add_run("L'indice accuse une tendance baissière sur la période, reflétant une pression vendeuse sur le marché.")
                else:
                    p_composite.add_run("L'indice évolue dans une phase de consolidation, sans tendance directrice nette sur la période.")
                
                # Commentaire capitalisation 100j
                cap_vals = df_hist['capitalisation_globale'].dropna()
                cap_vals = cap_vals[cap_vals > 0]
                if len(cap_vals) >= 2:
                    doc.add_paragraph()
                    p_cap = doc.add_paragraph()
                    p_cap.add_run("Capitalisation globale — ").bold = True
                    cap_first = float(cap_vals.iloc[0])
                    cap_last  = float(cap_vals.iloc[-1])
                    cap_max   = float(cap_vals.max())
                    cap_min   = float(cap_vals.min())
                    cap_evol  = ((cap_last - cap_first) / cap_first * 100)
                    div = 1e9

                    def fmt_mds(val):
                        """Formate une valeur en milliards avec séparateur français (3 décimales)"""
                        mds = val / div
                        entier = int(mds)
                        dec = round((mds - entier) * 1000)
                        return f"{entier:,}".replace(",", " ") + f",{dec:03d}"

                    p_cap.add_run("La capitalisation boursière totale a évolué de ")
                    cap_run = p_cap.add_run(f"{cap_evol:+.2f}%")
                    cap_run.bold = True
                    cap_run.font.color.rgb = RGBColor(0, 128, 0) if cap_evol >= 0 else RGBColor(192, 0, 0)
                    p_cap.add_run(
                        f" sur la période, passant de {fmt_mds(cap_first)} Mds FCFA à {fmt_mds(cap_last)} Mds FCFA. "
                        f"Le pic de capitalisation observé sur les 100 jours est de {fmt_mds(cap_max)} Mds FCFA "
                        f"et le plancher de {fmt_mds(cap_min)} Mds FCFA."
                    )
                
                doc.add_paragraph()
        
        # ========== TOP 10 ACHATS ==========
        doc.add_heading('📈 TOP 10 DES OPPORTUNITÉS D\'ACHAT', level=2)
        
        # ✅ TOP 10 achat = uniquement ACHAT FORT (score 5) et ACHAT (score 4), triés par score décroissant
        sorted_buy = sorted(
            [(symbol, data) for symbol, data in all_company_data.items()
             if data.get('recommendation_score', 0) >= 4],  # ACHAT ou ACHAT FORT uniquement
            key=lambda x: x[1].get('recommendation_score', 0),
            reverse=True
        )[:10]
        
        # Si moins de 10 ACHAT/ACHAT FORT, compléter avec CONSERVER trié par score
        if len(sorted_buy) < 10:
            conserver_list = sorted(
                [(symbol, data) for symbol, data in all_company_data.items()
                 if data.get('recommendation_score', 0) == 3
                 and symbol not in [s for s, _ in sorted_buy]],
                key=lambda x: x[1].get('recommendation_score', 0),
                reverse=True
            )
            sorted_buy = sorted_buy + conserver_list[:10 - len(sorted_buy)]
        
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
        doc.add_paragraph(
            "Ces actions présentent des signaux de vente ou de dégradation fondamentale. "
            "Il est conseillé de les céder immédiatement ou d'éviter d'y investir."
        )
        
        # ✅ TOP 10 à éviter = uniquement VENTE FORTE (score 1) et VENTE (score 2), pires en premier
        sorted_sell = sorted(
            [(symbol, data) for symbol, data in all_company_data.items()
             if data.get('recommendation_score', 3) <= 2],  # VENTE ou VENTE FORTE uniquement
            key=lambda x: x[1].get('recommendation_score', 3)
        )[:10]
        
        # Si moins de 10 VENTE/VENTE FORTE, compléter avec les CONSERVER les plus faibles (risk_score le plus élevé)
        if len(sorted_sell) < 10:
            conserver_risky = sorted(
                [(symbol, data) for symbol, data in all_company_data.items()
                 if data.get('recommendation_score', 3) == 3
                 and symbol not in [s for s, _ in sorted_sell]],
                key=lambda x: x[1].get('risk_score', 0),
                reverse=True
            )
            sorted_sell = sorted_sell + conserver_risky[:10 - len(sorted_sell)]
        
        flop_10_list = []
        if sorted_sell:
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
        else:
            p_empty = doc.add_paragraph()
            p_empty.add_run("✅ Aucune action à éviter ce jour.").italic = True
            p_empty.add_run(
                " L'ensemble des 47 sociétés analysées présentent des signaux neutres ou positifs. "
                "Aucun signal de vente fort n'a été détecté par l'analyse Multi-AI."
            )
        
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
        
        # Récupérer la perf BRVM Composite pour comparaison relative
        _brvm_perf = 0.0
        try:
            _mi = market_indicators_pre if 'market_indicators_pre' in dir() else self._get_market_indicators()
            if _mi and _mi.get('history_100d') is not None:
                _h = _mi['history_100d']
                if len(_h) >= 2:
                    _first = float(_h.iloc[0]['brvm_composite'])
                    _last  = float(_h.iloc[-1]['brvm_composite'])
                    _brvm_perf = ((_last - _first) / _first * 100) if _first else 0
        except Exception:
            _brvm_perf = 0.0

        for sector, stats in sorted(sector_analysis.items(), key=lambda x: x[1]['performance_moyenne'], reverse=True):
            doc.add_heading(f"Secteur: {sector}", level=3)

            p = doc.add_paragraph()
            p.add_run("Nombre de sociétés: ").bold = True
            p.add_run(f"{stats['nb_societes']}  |  ")

            # Performance vs BRVM Composite
            perf = stats['performance_moyenne']
            vs   = perf - _brvm_perf
            p.add_run("Performance moyenne (100j): ").bold = True
            perf_run = p.add_run(f"{perf:.2f}%")
            perf_run.font.color.rgb = RGBColor(0,128,0) if perf >= 0 else RGBColor(192,0,0)
            p.add_run("  |  ")
            p.add_run("vs BRVM Composite: ").bold = True
            vs_run = p.add_run(f"{'+'if vs>=0 else ''}{vs:.2f}%  "
                               + ("🟢 Surperformance" if vs > 0 else "🔴 Sous-performance"))
            vs_run.font.color.rgb = RGBColor(0,128,0) if vs >= 0 else RGBColor(192,0,0)
            vs_run.bold = True
            p.add_run("\n")

            p.add_run("Sentiment général: ").bold = True
            p.add_run(f"{stats['sentiment_general']}  |  ")
            p.add_run("Risque moyen: ").bold = True
            p.add_run(f"{stats['risque_moyen']}  |  ")
            p.add_run("Prix moyen: ").bold = True
            p.add_run(f"{stats['prix_moyen']:.0f} FCFA\n")

            p.add_run("\nSociétés: ").bold = True
            p.add_run(", ".join(stats['societes']))

            doc.add_paragraph()
        
        doc.add_page_break()
        
        # ========== 2. MATRICE DE CONVERGENCE ==========
        doc.add_heading('🔄 MATRICE DE CONVERGENCE DES SIGNAUX', level=1)
        
        doc.add_paragraph(
            "Cette matrice segmente les sociétés cotées selon la combinaison de leur signal technique "
            "(majorité des 5 indicateurs) et de leur signal fondamental (recommandation finale de l'IA). "
            "Les cellules surlignées en vert indiquent une convergence favorable, en rouge une convergence défavorable."
        )
        doc.add_paragraph()
        
        # ✅ Construction de la matrice 3x3 à partir de technical_decision × fundamental_decision
        matrix_3x3 = {
            ('ACHAT',  'ACHAT'):  [],
            ('ACHAT',  'NEUTRE'): [],
            ('ACHAT',  'VENTE'):  [],
            ('NEUTRE', 'ACHAT'):  [],
            ('NEUTRE', 'NEUTRE'): [],
            ('NEUTRE', 'VENTE'):  [],
            ('VENTE',  'ACHAT'):  [],
            ('VENTE',  'NEUTRE'): [],
            ('VENTE',  'VENTE'):  [],
        }
        
        for symbol, data in all_company_data.items():
            td = (data.get('technical_decision') or 'NEUTRE').upper()
            fd = (data.get('fundamental_decision') or 'NEUTRE').upper()
            # Normaliser
            if 'ACHAT' in td:
                td = 'ACHAT'
            elif 'VENTE' in td:
                td = 'VENTE'
            else:
                td = 'NEUTRE'
            if 'ACHAT' in fd:
                fd = 'ACHAT'
            elif 'VENTE' in fd:
                fd = 'VENTE'
            else:
                fd = 'NEUTRE'
            key = (td, fd)
            if key in matrix_3x3:
                matrix_3x3[key].append(symbol)
        
        # Construire le tableau Word 4×4 (en-têtes inclus)
        tbl = doc.add_table(rows=4, cols=4)
        tbl.style = 'Table Grid'
        
        # Couleurs
        GREEN_STRONG = '00B050'   # convergence achat
        GREEN_LIGHT  = 'C6EFCE'
        RED_STRONG   = 'FF0000'   # convergence vente
        RED_LIGHT    = 'FFC7CE'
        ORANGE       = 'FFEB9C'   # divergence tech/fond
        GREY_HDR     = 'D9D9D9'
        WHITE        = 'FFFFFF'
        
        def _set_cell_bg(cell, hex_color):
            shd = OxmlElement('w:shd')
            shd.set(qn('w:fill'), hex_color)
            shd.set(qn('w:val'), 'clear')
            cell._element.get_or_add_tcPr().append(shd)
        
        def _cell_text(cell, text, bold=False, font_size=9, color=None):
            cell.text = ''
            p = cell.paragraphs[0]
            run = p.add_run(text)
            run.bold = bold
            run.font.size = Pt(font_size)
            if color:
                run.font.color.rgb = RGBColor(*color)
            p.paragraph_format.space_before = Pt(2)
            p.paragraph_format.space_after = Pt(2)
        
        headers_col = ['', 'Fond. ACHAT', 'Fond. NEUTRE', 'Fond. VENTE']
        headers_row = ['Tech. ACHAT', 'Tech. NEUTRE', 'Tech. VENTE']
        
        # Couleurs de fond par cellule (td, fd)
        cell_colors = {
            ('ACHAT',  'ACHAT'):  GREEN_STRONG,
            ('ACHAT',  'NEUTRE'): GREEN_LIGHT,
            ('ACHAT',  'VENTE'):  ORANGE,
            ('NEUTRE', 'ACHAT'):  GREEN_LIGHT,
            ('NEUTRE', 'NEUTRE'): WHITE,
            ('NEUTRE', 'VENTE'):  RED_LIGHT,
            ('VENTE',  'ACHAT'):  ORANGE,
            ('VENTE',  'NEUTRE'): RED_LIGHT,
            ('VENTE',  'VENTE'):  RED_STRONG,
        }
        
        # Ligne d'en-tête (ligne 0)
        for col_idx, hdr in enumerate(headers_col):
            cell = tbl.rows[0].cells[col_idx]
            _set_cell_bg(cell, GREY_HDR)
            _cell_text(cell, hdr, bold=True, font_size=9)
        
        # Lignes 1-3
        tech_order = ['ACHAT', 'NEUTRE', 'VENTE']
        fund_order = ['ACHAT', 'NEUTRE', 'VENTE']
        
        for row_idx, td in enumerate(tech_order):
            row = tbl.rows[row_idx + 1]
            # Cellule d'en-tête de ligne
            _set_cell_bg(row.cells[0], GREY_HDR)
            _cell_text(row.cells[0], headers_row[row_idx], bold=True, font_size=9)
            
            for col_idx, fd in enumerate(fund_order):
                cell = row.cells[col_idx + 1]
                color = cell_colors.get((td, fd), WHITE)
                _set_cell_bg(cell, color)
                symbols_list = matrix_3x3.get((td, fd), [])
                content = ', '.join(symbols_list) if symbols_list else '—'
                _cell_text(cell, content, font_size=8)
        
        doc.add_paragraph()
        
        # Légende
        legend_p = doc.add_paragraph()
        legend_p.add_run("Légende : ").bold = True
        legend_p.add_run("🟩 Vert foncé = Double achat (signal fort) | 🟩 Vert clair = Signal partiellement haussier | "
                         "🟧 Orange = Divergence tech/fondamental | 🟥 Rouge clair = Signal partiellement baissier | "
                         "🟥 Rouge foncé = Double vente (éviter) | ⬜ Blanc = Neutre")
        legend_p.paragraph_format.space_after = Pt(6)
        doc.add_paragraph()
        
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

        # ================================================================
        # ========== ACTUALITÉS DU MARCHÉ (page dédiée) ==================
        # ================================================================
        doc.add_heading('📰 ACTUALITÉS DU MARCHÉ BRVM', level=1)
        doc.add_paragraph(
            "Cette section regroupe les derniers documents officiels publiés par les "
            "sociétés cotées (AG, dividendes, convocations, résultats) ainsi que les "
            "alertes d'actualité financière collectées via Google Alerts."
        ).runs[0].font.size = Pt(9)
        doc.add_paragraph()

        # ── A) Documents officiels BRVM : AG / dividendes / convocations ─────
        doc.add_heading('📄 A — Documents officiels des sociétés cotées', level=2)
        brvm_actu_df = self._get_brvm_actualites()

        if not brvm_actu_df.empty:
            # Grouper par catégorie pour structurer la section
            CAT_ICONS = {
                'ag':           ('🏛️',  'Assemblées Générales'),
                'dividende':    ('💰',  'Dividendes'),
                'convocation':  ('📩',  'Convocations'),
                'résultats':    ('📊',  'Résultats financiers'),
                'résultat':     ('📊',  'Résultats financiers'),
                'rapport':      ('📋',  'Rapports annuels'),
                'communiqué':   ('📢',  'Communiqués de presse'),
                'avis':         ('ℹ️',  'Avis & informations'),
            }

            # Grouper les documents par catégorie
            cat_groups = {}
            for _, row in brvm_actu_df.iterrows():
                cat_raw = str(row.get('categorie') or row.get('type_document') or row.get('rapport_type') or 'Autre').lower().strip()
                # Normaliser la catégorie
                cat_key = 'autre'
                for key in CAT_ICONS:
                    if key in cat_raw:
                        cat_key = key
                        break
                cat_groups.setdefault(cat_key, []).append(row)

            # Afficher par catégorie
            for cat_key, rows in sorted(cat_groups.items()):
                icon, cat_label = CAT_ICONS.get(cat_key, ('📌', cat_key.capitalize()))
                doc.add_heading(f"{icon} {cat_label} ({len(rows)} document(s))", level=3)

                # Tableau compact pour cette catégorie
                tbl = doc.add_table(rows=1, cols=5)
                tbl.style = 'Light Grid Accent 1'
                hdrs = ['Date', 'Société', 'Titre', 'Impact', 'Résumé']
                hcells = tbl.rows[0].cells
                for ci, h in enumerate(hdrs):
                    hcells[ci].text = h
                    r = hcells[ci].paragraphs[0].runs[0]
                    r.bold = True
                    r.font.size = Pt(8)
                    shd = OxmlElement('w:shd')
                    shd.set(qn('w:fill'), '1F4E79')
                    shd.set(qn('w:val'), 'clear')
                    hcells[ci]._element.get_or_add_tcPr().append(shd)
                    r.font.color.rgb = RGBColor(255,255,255)

                for row in rows[:10]:
                    tr = tbl.add_row().cells
                    # Date
                    date_val = str(row.get('date_doc') or row.get('date_publication') or '')[:10]
                    # Société
                    soc = str(row.get('societe_confirmee') or '')[:12]
                    # Titre
                    titre_doc = str(row.get('titre') or '')[:50]
                    # Impact
                    impact_raw = str(row.get('impact') or 'neutre').lower()
                    impact_map = {'positif':'🟢 +','negatif':'🔴 -','neutre':'⚪ ='}
                    impact_txt = impact_map.get(impact_raw, '⚪ =')
                    # Résumé court
                    res_court = str(row.get('resume') or '')[:120]

                    vals = [date_val, soc, titre_doc, impact_txt, res_court]
                    for ci, v in enumerate(vals):
                        tr[ci].text = v
                        run = tr[ci].paragraphs[0].runs[0] if tr[ci].paragraphs[0].runs else tr[ci].paragraphs[0].add_run(v)
                        run.font.size = Pt(7.5)
                        # Colorer impact
                        if ci == 3:
                            if '🟢' in v: run.font.color.rgb = RGBColor(0,128,0)
                            elif '🔴' in v: run.font.color.rgb = RGBColor(192,0,0)
                            run.bold = True

                    # Points clés en dessous si disponibles
                    pk = row.get('points_cles')
                    pts = []
                    if isinstance(pk, list): pts = [str(p) for p in pk[:3]]
                    elif isinstance(pk, str) and pk.strip():
                        try:
                            import json as _j
                            parsed = _j.loads(pk)
                            pts = [str(p) for p in parsed[:3]] if isinstance(parsed,list) else []
                        except Exception: pass

                    if pts:
                        pk_row = tbl.add_row().cells
                        # Fusionner les 5 colonnes pour les points clés
                        pk_para = pk_row[0].paragraphs[0]
                        pk_para.add_run("  → " + " | ".join(pts)).font.size = Pt(7)
                        pk_row[0].paragraphs[0].runs[-1].font.color.rgb = RGBColor(80,80,80)
                        # Étendre sur 5 cols (merge)
                        for merge_ci in range(1, 5):
                            pk_row[0].merge(pk_row[merge_ci])

                doc.add_paragraph()
        else:
            doc.add_paragraph("ℹ️ Aucun document officiel récent disponible dans la base de données.")

        doc.add_paragraph()

        # ── B) Alertes Google — actualités marché ────────────────────────────
        doc.add_heading('🔔 B — Alertes Google & actualités financières', level=2)
        alerts_df = self._get_google_alerts_events()

        if not alerts_df.empty:
            # Grouper par mot-clé / catégorie pour éviter la liste plate
            alert_groups = {}
            for _, alert_row in alerts_df.iterrows():
                kw = str(alert_row.get('alert_keyword') or alert_row.get('mot_cle') or
                         alert_row.get('categorie') or 'Marché BRVM').strip()
                alert_groups.setdefault(kw, []).append(alert_row)

            for kw, rows in alert_groups.items():
                grp_hdr = doc.add_paragraph()
                grp_hdr.paragraph_format.space_before = Pt(6)
                grp_hdr.paragraph_format.space_after  = Pt(2)
                rk = grp_hdr.add_run(f"🔑 {kw}  ({len(rows)} alerte(s))")
                rk.bold = True
                rk.font.size = Pt(10)
                rk.font.color.rgb = RGBColor(0,70,127)

                for alert_row in rows[:5]:
                    ap = doc.add_paragraph()
                    ap.paragraph_format.left_indent  = Pt(18)
                    ap.paragraph_format.space_before = Pt(2)
                    ap.paragraph_format.space_after  = Pt(2)

                    # Date
                    mail_date = alert_row.get('mail_date')
                    date_str  = mail_date.strftime('%d/%m/%Y') if hasattr(mail_date,'strftime') else str(mail_date)[:10]

                    # Sentiment
                    sent = str(alert_row.get('sentiment','') or '').lower()
                    s_color   = RGBColor(0,128,0) if 'positif' in sent else (RGBColor(192,0,0) if 'negatif' in sent else RGBColor(80,80,80))
                    sent_icon = '🟢' if 'positif' in sent else ('🔴' if 'negatif' in sent else '⚪')

                    # Titre : mail_subject > titre > resume[:80]
                    titre_a = str(
                        alert_row.get('mail_subject') or
                        alert_row.get('titre') or
                        str(alert_row.get('resume',''))[:80]
                    )[:120]

                    resume_a = str(alert_row.get('resume') or '')[:350]
                    cat_a    = str(alert_row.get('categorie') or alert_row.get('rapport_type') or '')
                    pert_a   = alert_row.get('pertinence')
                    url_a    = str(alert_row.get('source_url') or '')[:80]

                    # Ligne titre
                    rd = ap.add_run(f"[{date_str}] {sent_icon}  ")
                    rd.font.size = Pt(8.5)
                    rd.font.color.rgb = RGBColor(100,100,100)
                    rt = ap.add_run(titre_a)
                    rt.bold = True
                    rt.font.size = Pt(9)
                    rt.font.color.rgb = s_color

                    if cat_a:
                        rc = ap.add_run(f"  [{cat_a}]")
                        rc.font.size = Pt(7.5)
                        rc.font.color.rgb = RGBColor(120,120,120)

                    if pert_a and pd.notna(pert_a):
                        rp = ap.add_run(f"  ★ {int(pert_a)}/10")
                        rp.font.size = Pt(8)
                        rp.font.color.rgb = RGBColor(180,120,0)

                    # Résumé sur la ligne suivante
                    if resume_a and resume_a.strip() and resume_a[:80] not in titre_a:
                        ap_res = doc.add_paragraph()
                        ap_res.paragraph_format.left_indent  = Pt(30)
                        ap_res.paragraph_format.space_before = Pt(0)
                        ap_res.paragraph_format.space_after  = Pt(1)
                        rr = ap_res.add_run(resume_a)
                        rr.font.size = Pt(8.5)
                        rr.font.color.rgb = RGBColor(50,50,50)

                    # Points clés de l'alerte
                    pk_a = alert_row.get('points_cles')
                    pts_a = []
                    if isinstance(pk_a, list): pts_a = [str(p) for p in pk_a[:4]]
                    elif isinstance(pk_a, str) and pk_a.strip():
                        try:
                            import json as _jj
                            parsed = _jj.loads(pk_a)
                            pts_a = [str(p) for p in parsed[:4]] if isinstance(parsed,list) else []
                        except Exception: pass

                    if pts_a:
                        pk_para = doc.add_paragraph()
                        pk_para.paragraph_format.left_indent  = Pt(30)
                        pk_para.paragraph_format.space_before = Pt(0)
                        pk_para.paragraph_format.space_after  = Pt(2)
                        pk_run = pk_para.add_run("Points clés : " + "  •  ".join(pts_a))
                        pk_run.font.size = Pt(8)
                        pk_run.font.color.rgb = RGBColor(0,100,60)

                    # URL source
                    if url_a and url_a.startswith('http'):
                        url_para = doc.add_paragraph()
                        url_para.paragraph_format.left_indent  = Pt(30)
                        url_para.paragraph_format.space_before = Pt(0)
                        url_para.paragraph_format.space_after  = Pt(3)
                        ur = url_para.add_run(f"Source : {url_a}")
                        ur.font.size = Pt(7.5)
                        ur.font.color.rgb = RGBColor(0,100,180)

        else:
            doc.add_paragraph("ℹ️ Aucune alerte Google disponible pour cette période.")

        doc.add_page_break()

        # ========== CLASSEMENT 47 SOCIÉTÉS PAR SCORE COMPOSITE ==========
        doc.add_heading('🏆 CLASSEMENT DES SOCIÉTÉS — SCORE COMPOSITE /100', level=1)
        doc.add_paragraph(
            "Les 47 sociétés sont classées par score composite (Technique 30% + Fondamental 40% "
            "+ Risque 20% + Liquidité 10%). Ce tableau est l'outil de référence pour une décision rapide."
        )
        doc.add_paragraph()

        # Trier par score décroissant
        ranked = sorted(
            [(sym, d) for sym, d in all_company_data.items()],
            key=lambda x: x[1].get('investment_score', 0),
            reverse=True
        )

        score_tbl = doc.add_table(rows=1, cols=8)
        score_tbl.style = 'Light Grid Accent 1'
        hdrs = ['#', 'Symbole', 'Secteur', 'Prix (FCFA)', 'Score /100', 'Signal Tech', 'Signal Fond', 'Recommandation']
        hcells = score_tbl.rows[0].cells
        for ci, h in enumerate(hdrs):
            hcells[ci].text = h
            hcells[ci].paragraphs[0].runs[0].bold = True
            hcells[ci].paragraphs[0].runs[0].font.size = Pt(8.5)
            shd = OxmlElement('w:shd')
            shd.set(qn('w:fill'), '1F4E79')
            shd.set(qn('w:val'), 'clear')
            hcells[ci]._element.get_or_add_tcPr().append(shd)
            hcells[ci].paragraphs[0].runs[0].font.color.rgb = RGBColor(255,255,255)

        for rank, (sym, d) in enumerate(ranked, 1):
            row_cells = score_tbl.add_row().cells
            sc = d.get('investment_score', 0)
            lbl = d.get('investment_label','—')
            rec = d.get('recommendation','—')
            td  = d.get('technical_decision','—')
            fd  = d.get('fundamental_decision','—')

            # Couleur ligne selon score
            if   sc >= 70: row_bg = 'C6EFCE'
            elif sc >= 55: row_bg = 'EBF5FB'
            elif sc >= 40: row_bg = 'FEFEFE'
            else:          row_bg = 'FCE4D6'

            vals = [
                str(rank),
                sym,
                str(d.get('sector','—') or '—')[:18],
                f"{d.get('current_price',0):,.0f}" if d.get('current_price') else '—',
                f"{sc}/100  ({lbl})",
                td,
                fd,
                rec,
            ]
            for ci, val in enumerate(vals):
                row_cells[ci].text = val
                run = row_cells[ci].paragraphs[0].runs[0] if row_cells[ci].paragraphs[0].runs else row_cells[ci].paragraphs[0].add_run(val)
                run.font.size = Pt(8)
                shd2 = OxmlElement('w:shd')
                shd2.set(qn('w:fill'), row_bg)
                shd2.set(qn('w:val'), 'clear')
                row_cells[ci]._element.get_or_add_tcPr().append(shd2)
                # Coloration recommandation
                if ci == 7:
                    if   'ACHAT' in rec.upper(): run.font.color.rgb = RGBColor(0,128,0)
                    elif 'VENTE' in rec.upper(): run.font.color.rgb = RGBColor(192,0,0)
                # Score en gras
                if ci == 4:
                    run.bold = True

        doc.add_paragraph()
        doc.add_page_break()

        # ========== PORTEFEUILLES MODÈLES ==========
        doc.add_heading('💼 PORTEFEUILLES MODÈLES', level=1)
        doc.add_paragraph(
            "Trois portefeuilles construits automatiquement selon le score composite, "
            "le niveau de risque et la liquidité. Les pondérations sont indicatives et "
            "équipondérées. Position cash recommandée incluse."
        )
        doc.add_paragraph()

        portfolios = self._build_model_portfolios(all_company_data)
        port_configs = [
            ('defensif',  '🔵 Portefeuille DÉFENSIF',
             'Faible risque, haute liquidité, score ≥ 45. Adapté aux investisseurs prudents.'),
            ('equilibre', '🟢 Portefeuille ÉQUILIBRÉ',
             'Risque faible à moyen, score ≥ 50. Le meilleur rapport rendement/risque.'),
            ('offensif',  '🔴 Portefeuille OFFENSIF',
             'Meilleurs scores (≥ 60), tous niveaux de risque. Pour investisseurs avertis.'),
        ]
        PORT_COLORS = {'defensif':'DEEAF1','equilibre':'E2EFDA','offensif':'FCE4D6'}

        for port_key, port_title, port_desc in port_configs:
            entries_w, cash_pct = portfolios[port_key]
            doc.add_heading(port_title, level=3)
            dp = doc.add_paragraph(port_desc)
            dp.runs[0].font.size = Pt(9)
            dp.runs[0].font.italic = True

            if not entries_w:
                doc.add_paragraph("ℹ️ Aucun titre ne satisfait les critères ce jour.")
                doc.add_paragraph()
                continue

            # Tableau du portefeuille
            ptbl = doc.add_table(rows=1, cols=5)
            ptbl.style = 'Light Grid Accent 1'
            phdrs = ['Symbole', 'Société', 'Prix (FCFA)', 'Score /100', 'Poids %']
            phcells = ptbl.rows[0].cells
            for ci, ph in enumerate(phdrs):
                phcells[ci].text = ph
                phcells[ci].paragraphs[0].runs[0].bold = True
                phcells[ci].paragraphs[0].runs[0].font.size = Pt(8.5)
                pshd = OxmlElement('w:shd')
                pshd.set(qn('w:fill'), '2E74B5')
                pshd.set(qn('w:val'), 'clear')
                phcells[ci]._element.get_or_add_tcPr().append(pshd)
                phcells[ci].paragraphs[0].runs[0].font.color.rgb = RGBColor(255,255,255)

            for entry, weight in entries_w:
                pr = ptbl.add_row().cells
                vals = [
                    entry['sym'],
                    (entry['name'] or '')[:28],
                    f"{entry['price']:,.0f}" if entry['price'] else '—',
                    f"{entry['score']}/100",
                    f"{weight}%",
                ]
                for ci, vp in enumerate(vals):
                    pr[ci].text = vp
                    run = pr[ci].paragraphs[0].runs[0] if pr[ci].paragraphs[0].runs else pr[ci].paragraphs[0].add_run(vp)
                    run.font.size = Pt(8.5)
                    pshd2 = OxmlElement('w:shd')
                    pshd2.set(qn('w:fill'), PORT_COLORS.get(port_key,'FFFFFF'))
                    pshd2.set(qn('w:val'), 'clear')
                    pr[ci]._element.get_or_add_tcPr().append(pshd2)

            # Ligne cash
            cash_row = ptbl.add_row().cells
            cash_vals = ['CASH', 'Liquidités', '—', '—', f"{cash_pct}%"]
            for ci, vp in enumerate(cash_vals):
                cash_row[ci].text = vp
                run = cash_row[ci].paragraphs[0].runs[0] if cash_row[ci].paragraphs[0].runs else cash_row[ci].paragraphs[0].add_run(vp)
                run.font.size = Pt(8.5)
                run.font.italic = True
                run.font.color.rgb = RGBColor(80,80,80)

            doc.add_paragraph()

        doc.add_page_break()

        # ========== ALERTES DU JOUR ==========
        doc.add_heading('⚡ ALERTES DU JOUR', level=1)
        doc.add_paragraph(
            "Sociétés présentant des signaux extrêmes ou des incohérences majeures "
            "nécessitant une attention particulière avant toute décision."
        )
        doc.add_paragraph()

        alerts_list = []
        for sym, d in all_company_data.items():
            rsi_val  = d.get('rsi_value')
            price    = d.get('current_price') or 0
            high100  = d.get('highest_price_100d') or 0
            low100   = d.get('lowest_price_100d') or 0
            td       = str(d.get('technical_decision','')).upper()
            fd       = str(d.get('fundamental_decision','')).upper()
            rec      = str(d.get('recommendation','')).upper()

            alert_msgs = []

            # RSI extrêmes
            if rsi_val and pd.notna(rsi_val):
                rsi_f = float(rsi_val)
                if rsi_f > 70:
                    alert_msgs.append(f"🔴 RSI surachat ({rsi_f:.1f} > 70) — risque de correction")
                elif rsi_f < 30:
                    alert_msgs.append(f"🟢 RSI survente ({rsi_f:.1f} < 30) — opportunité potentielle")

            # Cours proche des bornes 100j
            if price and high100 and high100 > 0:
                dist_high = (high100 - price) / high100 * 100
                if dist_high < 3:
                    alert_msgs.append(f"⚠️ Cours à {dist_high:.1f}% du plus haut 100j ({high100:,.0f} FCFA)")
            if price and low100 and low100 > 0:
                dist_low = (price - low100) / low100 * 100
                if dist_low < 3:
                    alert_msgs.append(f"⚠️ Cours à {dist_low:.1f}% du plus bas 100j ({low100:,.0f} FCFA)")

            # Incohérence tech ↔ fond + recommandation ⚠️
            if '⚠️' in rec or (
                ('ACHAT' in td and 'VENTE' in fd) or
                ('VENTE' in td and 'ACHAT' in fd)
            ):
                alert_msgs.append(
                    f"🟠 Divergence tech({td}) / fond({fd}) — recommandation ajustée à {rec}"
                )

            if alert_msgs:
                alerts_list.append((sym, d.get('company_name',''), alert_msgs))

        if alerts_list:
            for sym, cname, msgs in sorted(alerts_list, key=lambda x: len(x[2]), reverse=True):
                ahdr = doc.add_paragraph()
                ahdr.paragraph_format.space_before = Pt(6)
                ahdr.add_run(f"{sym} — {cname}").bold = True
                ahdr.runs[-1].font.size = Pt(10)
                ahdr.runs[-1].font.color.rgb = RGBColor(0,80,160)
                for msg in msgs:
                    am = doc.add_paragraph(style='List Bullet')
                    am.paragraph_format.left_indent = Pt(20)
                    am.paragraph_format.space_before = Pt(1)
                    am.add_run(msg).font.size = Pt(9)
        else:
            doc.add_paragraph("✅ Aucune alerte majeure ce jour.")

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
            
            # ✅ Tableau des métriques de risque (risk_details)
            risk_details_raw = company_data.get('risk_details', '{}')
            try:
                risk_details = json.loads(risk_details_raw) if isinstance(risk_details_raw, str) else risk_details_raw
            except Exception:
                risk_details = {}
            
            if risk_details:
                risk_label_map = {
                    'volatilite': 'Volatilité',
                    'beta': 'Bêta',
                    'liquidite': 'Liquidité',
                    'divergence': 'Divergence signaux',
                    'stabilite': 'Stabilité des rendements'
                }
                risk_tbl = doc.add_table(rows=1, cols=len(risk_details))
                risk_tbl.style = 'Light Grid Accent 1'
                hdr_row = risk_tbl.rows[0].cells
                metrics_order = ['volatilite', 'beta', 'liquidite', 'divergence', 'stabilite']
                ordered_keys = [k for k in metrics_order if k in risk_details] + \
                               [k for k in risk_details if k not in metrics_order]
                for col_i, key in enumerate(ordered_keys[:len(hdr_row)]):
                    shd = OxmlElement('w:shd')
                    shd.set(qn('w:fill'), 'BDD7EE')
                    hdr_row[col_i]._element.get_or_add_tcPr().append(shd)
                    hdr_row[col_i].text = risk_label_map.get(key, key.capitalize())
                    hdr_row[col_i].paragraphs[0].runs[0].font.bold = True
                    hdr_row[col_i].paragraphs[0].runs[0].font.size = Pt(8)
                
                val_row = risk_tbl.add_row().cells
                for col_i, key in enumerate(ordered_keys[:len(val_row)]):
                    val_row[col_i].text = str(risk_details.get(key, '—'))
                    val_row[col_i].paragraphs[0].runs[0].font.size = Pt(8)
                
                doc.add_paragraph()

            # ── Feux tricolores indicateurs techniques ──────────────────────────
            tech_indicators = [
                ('MM',          company_data.get('mm_decision')),
                ('Bollinger',   company_data.get('bollinger_decision')),
                ('MACD',        company_data.get('macd_decision')),
                ('RSI',         company_data.get('rsi_decision')),
                ('Stochastique',company_data.get('stochastic_decision')),
            ]
            feux_p = doc.add_paragraph()
            feux_p.paragraph_format.space_before = Pt(4)
            feux_p.paragraph_format.space_after  = Pt(2)
            feux_p.add_run("Signaux techniques : ").bold = True
            for ind_name, ind_val in tech_indicators:
                iv = str(ind_val or '').upper()
                if   'ACHAT' in iv: icon, col = '🟢', RGBColor(0,128,0)
                elif 'VENTE' in iv: icon, col = '🔴', RGBColor(192,0,0)
                else:               icon, col = '🟡', RGBColor(160,120,0)
                r_icon = feux_p.add_run(f"{icon} {ind_name}  ")
                r_icon.font.size = Pt(9)
                r_icon.font.color.rgb = col

            # ── Score composite bandeau ──────────────────────────────────────────
            inv_score = company_data.get('investment_score', 0)
            inv_label = company_data.get('investment_label', '—')
            score_p = doc.add_paragraph()
            score_p.paragraph_format.space_before = Pt(4)
            score_p.paragraph_format.space_after  = Pt(4)
            sr1 = score_p.add_run("Score composite : ")
            sr1.bold = True
            sr1.font.size = Pt(11)
            sr2 = score_p.add_run(f"{inv_score}/100  ({inv_label})")
            sr2.bold = True
            sr2.font.size = Pt(13)
            if   inv_score >= 70: sr2.font.color.rgb = RGBColor(0,128,0)
            elif inv_score >= 55: sr2.font.color.rgb = RGBColor(0,100,160)
            elif inv_score >= 40: sr2.font.color.rgb = RGBColor(160,100,0)
            else:                 sr2.font.color.rgb = RGBColor(192,0,0)
            doc.add_paragraph()

            # ── Graphique cours + volumes ────────────────────────────────────────
            hist_df_chart = self._get_historical_data_100days(company_data.get('company_id'))
            chart_buf = self._generate_price_chart(symbol, hist_df_chart)
            if chart_buf:
                try:
                    doc.add_picture(chart_buf, width=Inches(6.2))
                    last_pic = doc.paragraphs[-1]
                    last_pic.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    doc.add_paragraph()
                except Exception as ce:
                    logging.warning(f"⚠️  Insertion image {symbol}: {ce}")

            # ── Tableau prédictions J+1 → J+10 avec IC ──────────────────────────
            preds_full = company_data.get('predictions_full', [])
            current_price = company_data.get('current_price') or 0
            if preds_full:
                doc.add_heading('🔮 Prédictions J+1 à J+10 (Intervalle de Confiance 90%)', level=3)
                pred_tbl = doc.add_table(rows=1, cols=6)
                pred_tbl.style = 'Light Grid Accent 1'
                pred_hdrs = ['Jour', 'Date', 'Borne basse', 'Prix prédit', 'Borne haute', 'Var. % / actuel']
                ph_cells = pred_tbl.rows[0].cells
                for ci, ph in enumerate(pred_hdrs):
                    ph_cells[ci].text = ph
                    r = ph_cells[ci].paragraphs[0].runs[0]
                    r.bold = True
                    r.font.size = Pt(8.5)
                    shd_ph = OxmlElement('w:shd')
                    shd_ph.set(qn('w:fill'), '1F4E79')
                    shd_ph.set(qn('w:val'), 'clear')
                    ph_cells[ci]._element.get_or_add_tcPr().append(shd_ph)
                    r.font.color.rgb = RGBColor(255,255,255)

                for ji, pred in enumerate(preds_full[:10], 1):
                    pr = pred_tbl.add_row().cells
                    pprice = pred.get('price') or 0
                    plower = pred.get('lower')
                    pupper = pred.get('upper')
                    pconf  = pred.get('confidence','')
                    pdate  = str(pred.get('date',''))[:10]
                    var_pct= ((pprice - current_price) / current_price * 100) if current_price else 0

                    # Couleur ligne
                    if   var_pct >  2: row_cl = 'C6EFCE'
                    elif var_pct < -2: row_cl = 'FFDCE0'
                    else:              row_cl = 'FAFAFA'

                    conf_stars = {'Élevée':'⭐⭐⭐','Moyen':'⭐⭐','Faible':'⭐'}.get(pconf, pconf)

                    vals_p = [
                        f"J+{ji}  {conf_stars}",
                        pdate,
                        f"{plower:,.0f}" if plower else '—',
                        f"{pprice:,.0f}",
                        f"{pupper:,.0f}" if pupper else '—',
                        f"{'+'if var_pct>=0 else ''}{var_pct:.1f}%",
                    ]
                    for ci, vp in enumerate(vals_p):
                        pr[ci].text = vp
                        run_p = pr[ci].paragraphs[0].runs[0] if pr[ci].paragraphs[0].runs else pr[ci].paragraphs[0].add_run(vp)
                        run_p.font.size = Pt(8.5)
                        shd_pr = OxmlElement('w:shd')
                        shd_pr.set(qn('w:fill'), row_cl)
                        shd_pr.set(qn('w:val'), 'clear')
                        pr[ci]._element.get_or_add_tcPr().append(shd_pr)
                        if ci == 3:   # Prix prédit en gras
                            run_p.bold = True
                        if ci == 5:   # Variation colorée
                            run_p.font.color.rgb = RGBColor(0,128,0) if var_pct >= 0 else RGBColor(192,0,0)
                            run_p.bold = True

                doc.add_paragraph()
                doc.add_paragraph(
                    "⚠️ Les prédictions sont des estimations statistiques (modèles GRU/LSTM). "
                    "L'intervalle de confiance à 90% indique que 9 fois sur 10, le cours devrait "
                    "se situer entre la borne basse et la borne haute. ⭐⭐⭐ = confiance élevée."
                ).runs[0].font.size = Pt(8)
                doc.add_paragraph()

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

            # ═══════ SECTION brvm_documents ═══════════════════════════════════════
            brvm_docs_raw = company_data.get('brvm_docs_raw', [])
            if not brvm_docs_raw:
                # Fallback: chercher dans all_analyses via data_dict (cas où non propagé)
                pass
            if brvm_docs_raw:
                doc.add_paragraph()
                hdg = doc.add_heading('📎 DOCUMENTS & COMMUNICATIONS OFFICIELS BRVM', level=3)
                hdg.runs[0].font.color.rgb = RGBColor(0, 70, 127)

                nb_docs_display = min(len(brvm_docs_raw), 5)
                intro_p = doc.add_paragraph()
                intro_run = intro_p.add_run(
                    f"{len(brvm_docs_raw)} document(s) disponible(s) pour cette société "
                    f"(affichage des {nb_docs_display} plus récents)."
                )
                intro_run.font.size = Pt(9)
                intro_run.font.italic = True
                intro_run.font.color.rgb = RGBColor(100, 100, 100)

                for doc_i, raw_doc in enumerate(brvm_docs_raw[:5], 1):
                    d = self._format_brvm_documents_for_word(raw_doc, doc_i)

                    # En-tête du document
                    doc_hdr = doc.add_paragraph()
                    doc_hdr.paragraph_format.space_before = Pt(8)
                    run_num = doc_hdr.add_run(f"[{d['num']}] ")
                    run_num.bold = True
                    run_num.font.size = Pt(10)
                    run_num.font.color.rgb = RGBColor(0, 102, 204)
                    run_titre = doc_hdr.add_run(d['titre'])
                    run_titre.bold = True
                    run_titre.font.size = Pt(10)

                    # Méta-ligne : date + catégorie + impact
                    meta_p = doc.add_paragraph()
                    meta_p.paragraph_format.left_indent = Pt(20)
                    meta_p.paragraph_format.space_before = Pt(1)
                    meta_run = meta_p.add_run(
                        f"Date : {d['date_doc']}  |  Catégorie : {d['categorie']}  |  Impact : "
                    )
                    meta_run.font.size = Pt(8.5)
                    meta_run.font.color.rgb = RGBColor(80, 80, 80)
                    impact_run = meta_p.add_run(d['impact'])
                    impact_run.font.size = Pt(8.5)
                    impact_run.bold = True
                    impact_run.font.color.rgb = d['impact_color']

                    # Résumé
                    if d['resume']:
                        resume_p = doc.add_paragraph()
                        resume_p.paragraph_format.left_indent = Pt(20)
                        resume_p.paragraph_format.space_before = Pt(2)
                        resume_run = resume_p.add_run(d['resume'][:700])
                        resume_run.font.size = Pt(9)
                        if len(d['resume']) > 700:
                            resume_p.add_run("…").font.size = Pt(9)

                    # Points clés (si disponibles)
                    if d['points']:
                        pk_hdr = doc.add_paragraph()
                        pk_hdr.paragraph_format.left_indent = Pt(20)
                        pk_hdr.paragraph_format.space_before = Pt(3)
                        pk_hdr.add_run("Points clés :").bold = True
                        pk_hdr.runs[-1].font.size = Pt(9)
                        for pt in d['points']:
                            pk_p = doc.add_paragraph(style='List Bullet')
                            pk_p.paragraph_format.left_indent = Pt(35)
                            pk_p.paragraph_format.space_before = Pt(1)
                            pk_p.paragraph_format.space_after = Pt(1)
                            pk_run = pk_p.add_run(str(pt)[:200])
                            pk_run.font.size = Pt(9)

                    # Séparateur léger entre documents
                    if doc_i < nb_docs_display:
                        sep_p = doc.add_paragraph()
                        sep_p.paragraph_format.left_indent = Pt(20)
                        sep_run = sep_p.add_run("· " * 30)
                        sep_run.font.size = Pt(7)
                        sep_run.font.color.rgb = RGBColor(180, 180, 180)


            # ═══════ SECTION brvm_rapports_societes ═══════════════════════════════
            brvm_rapports_raw = company_data.get('brvm_rapports_raw', [])
            if brvm_rapports_raw:
                doc.add_paragraph()
                hdg2 = doc.add_heading('📋 RAPPORTS & COMMUNIQUÉS OFFICIELS', level=3)
                hdg2.runs[0].font.color.rgb = RGBColor(0, 90, 50)

                nb_rap_display = min(len(brvm_rapports_raw), 5)
                intro2 = doc.add_paragraph()
                ir = intro2.add_run(
                    f"{len(brvm_rapports_raw)} rapport(s)/communiqué(s) disponible(s) "
                    f"(affichage des {nb_rap_display} plus récents)."
                )
                ir.font.size = Pt(9)
                ir.font.italic = True
                ir.font.color.rgb = RGBColor(100, 100, 100)

                for ri, raw_rap in enumerate(brvm_rapports_raw[:5], 1):
                    r = self._format_rapports_societes_for_word(raw_rap, ri)

                    # ── En-tête ──────────────────────────────────────────────────
                    rhdr = doc.add_paragraph()
                    rhdr.paragraph_format.space_before = Pt(8)
                    run_ri = rhdr.add_run(f"[{r['idx']}] ")
                    run_ri.bold = True
                    run_ri.font.size = Pt(10)
                    run_ri.font.color.rgb = RGBColor(0, 120, 60)
                    run_rt = rhdr.add_run(r['doc_titre'])
                    run_rt.bold = True
                    run_rt.font.size = Pt(10)

                    # ── Méta ─────────────────────────────────────────────────────
                    rmeta = doc.add_paragraph()
                    rmeta.paragraph_format.left_indent = Pt(20)
                    rmeta.paragraph_format.space_before = Pt(1)
                    meta_txt = f"Année : {r['annee']}"
                    if r['type_rapport']:
                        meta_txt += f"  |  Type : {r['type_rapport']}"
                    if r['date_rapport']:
                        meta_txt += f"  |  Date : {r['date_rapport']}"
                    rmr = rmeta.add_run(meta_txt)
                    rmr.font.size = Pt(8.5)
                    rmr.font.color.rgb = RGBColor(80, 80, 80)

                    # ── Recommandation source ────────────────────────────────────
                    if r['recommandation']:
                        rrec = doc.add_paragraph()
                        rrec.paragraph_format.left_indent = Pt(20)
                        rrec.paragraph_format.space_before = Pt(1)
                        rrec.add_run("Recommandation source : ").font.size = Pt(9)
                        rrec.runs[-1].bold = True
                        rec_run = rrec.add_run(r['recommandation'])
                        rec_run.font.size = Pt(9)
                        rec_run.bold = True
                        rec_run.font.color.rgb = r['rec_color']

                    # ── Résumé ───────────────────────────────────────────────────
                    if r['resume']:
                        rres = doc.add_paragraph()
                        rres.paragraph_format.left_indent = Pt(20)
                        rres.paragraph_format.space_before = Pt(2)
                        rres_run = rres.add_run(r['resume'][:700])
                        rres_run.font.size = Pt(9)
                        if len(r['resume']) > 700:
                            rres.add_run("…").font.size = Pt(9)

                    # ── Indicateurs financiers ───────────────────────────────────
                    if r['indicateurs']:
                        rind_hdr = doc.add_paragraph()
                        rind_hdr.paragraph_format.left_indent = Pt(20)
                        rind_hdr.paragraph_format.space_before = Pt(3)
                        rind_hdr_run = rind_hdr.add_run("Indicateurs financiers :")
                        rind_hdr_run.bold = True
                        rind_hdr_run.font.size = Pt(9)

                        # Tableau compact indicateurs
                        ind_items = list(r['indicateurs'].items())
                        if ind_items:
                            ind_tbl = doc.add_table(rows=1, cols=min(len(ind_items), 4))
                            ind_tbl.style = 'Light Grid Accent 1'
                            ind_tbl.paragraph_format = None
                            hcells = ind_tbl.rows[0].cells
                            for ci, (k, v) in enumerate(ind_items[:4]):
                                hcells[ci].text = ''
                                ph = hcells[ci].paragraphs[0]
                                ph.paragraph_format.left_indent = Pt(5)
                                rk = ph.add_run(f"{k}\n")
                                rk.bold = True
                                rk.font.size = Pt(8)
                                rv = ph.add_run(str(v))
                                rv.font.size = Pt(9)
                            # Deuxième ligne si > 4 indicateurs
                            if len(ind_items) > 4:
                                row2 = ind_tbl.add_row().cells
                                for ci, (k, v) in enumerate(ind_items[4:8]):
                                    row2[ci].text = ''
                                    ph2 = row2[ci].paragraphs[0]
                                    rk2 = ph2.add_run(f"{k}\n")
                                    rk2.bold = True
                                    rk2.font.size = Pt(8)
                                    rv2 = ph2.add_run(str(v))
                                    rv2.font.size = Pt(9)
                            doc.add_paragraph()

                    # ── Points clés ──────────────────────────────────────────────
                    if r['points']:
                        rpk_hdr = doc.add_paragraph()
                        rpk_hdr.paragraph_format.left_indent = Pt(20)
                        rpk_hdr.paragraph_format.space_before = Pt(3)
                        rpk_hdr.add_run("Points clés :").bold = True
                        rpk_hdr.runs[-1].font.size = Pt(9)
                        for pt in r['points']:
                            rpk_p = doc.add_paragraph(style='List Bullet')
                            rpk_p.paragraph_format.left_indent = Pt(35)
                            rpk_p.paragraph_format.space_before = Pt(1)
                            rpk_p.paragraph_format.space_after = Pt(1)
                            rpk_p.add_run(str(pt)[:200]).font.size = Pt(9)

                    # ── Risques ──────────────────────────────────────────────────
                    if r['risques']:
                        rrisk = doc.add_paragraph()
                        rrisk.paragraph_format.left_indent = Pt(20)
                        rrisk.paragraph_format.space_before = Pt(2)
                        rrisk.add_run("⚠️ Risques : ").bold = True
                        rrisk.runs[-1].font.size = Pt(9)
                        rrisk.runs[-1].font.color.rgb = RGBColor(192, 80, 0)
                        rrisk.add_run(r['risques'][:300]).font.size = Pt(9)

                    # ── Perspectives ─────────────────────────────────────────────
                    if r['perspectives']:
                        rpersp = doc.add_paragraph()
                        rpersp.paragraph_format.left_indent = Pt(20)
                        rpersp.paragraph_format.space_before = Pt(2)
                        rpersp.add_run("🔮 Perspectives : ").bold = True
                        rpersp.runs[-1].font.size = Pt(9)
                        rpersp.runs[-1].font.color.rgb = RGBColor(0, 100, 160)
                        rpersp.add_run(r['perspectives'][:300]).font.size = Pt(9)

                    # ── Séparateur ───────────────────────────────────────────────
                    if ri < nb_rap_display:
                        sep_rp = doc.add_paragraph()
                        sep_rp.paragraph_format.left_indent = Pt(20)
                        sep_rp.add_run("· " * 30).font.size = Pt(7)
                        sep_rp.runs[-1].font.color.rgb = RGBColor(180, 180, 180)

            # ═══════════════════════════════════════════════════════════════════════
            # ═══════════════════════════════════════════════════════════════════════
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
            f"10. Rapport généré automatiquement - Version 30.2 Ultimate - {len(all_analyses)} sociétés analysées."
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
            cap_val = market_indicators.get('capitalisation', 0)
            cap_mds = cap_val / 1e9
            cap_mds_entier = int(cap_mds)
            cap_mds_dec = round((cap_mds - cap_mds_entier) * 1000)
            cap_mds_display = f"{cap_mds_entier:,}".replace(",", " ") + f",{cap_mds_dec:03d}"
            synthesis_text = (
                f"Analyse ULTIMATE de {len(all_analyses)} sociétés. "
                f"Indices: BRVM Composite {market_indicators['composite']:.2f} pts. "
                f"Capitalisation: {cap_mds_display} Mds FCFA. "
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
        logging.info("📝 ÉTAPE 5: GÉNÉRATION RAPPORTS (V30.2 ULTIMATE)")
        logging.info("🤖 Multi-AI: DeepSeek → Gemini → Mistral")
        logging.info("📊 Analyses: Sectorielles + Convergence + Liquidité + Divergences + Risque/Horizon")
        logging.info("="*80)
        
        # Vérifier qu'au moins une clé API est disponible
        if not any([DEEPSEEK_API_KEY, GEMINI_API_KEY, MISTRAL_API_KEY]):
            logging.error("❌ Aucune clé API configurée!")
            return
        
        available_apis = []
        missing_apis = []
        if DEEPSEEK_API_KEY:
            available_apis.append("DeepSeek")
        else:
            missing_apis.append("DeepSeek")
        if GEMINI_API_KEY:
            available_apis.append("Gemini")
        else:
            missing_apis.append("Gemini")
        if MISTRAL_API_KEY:
            available_apis.append("Mistral")
        else:
            missing_apis.append("Mistral")
        
        logging.info(f"✅ API disponibles: {', '.join(available_apis)}")
        if missing_apis:
            logging.warning(f"⚠️  API non configurées (ajouter DEEPSEEK_API_KEY dans GitHub Secrets): {', '.join(missing_apis)}")
        
        df = self._get_all_data_from_db()
        
        if df.empty:
            logging.error("❌ Aucune donnée disponible")
            return
        
        predictions_df = self._get_predictions_from_db()
        brvm_docs_by_symbol     = self._get_brvm_documents()
        brvm_rapports_by_symbol = self._get_brvm_rapports_societes()
        
        logging.info(f"🤖 Génération de {len(df)} analyse(s) avec rotation Multi-AI...")

        # ── Fonction locale : confiance dynamique ─────────────────────────────
        def _dynamic_confidence(tech_dec, fund_dec, rec_score):
            """
            Calcule un niveau de confiance selon la convergence tech / fondamental.
            Élevée  : tech et fond convergent ET score ≥ 4
            Faible  : forte divergence (ACHAT vs VENTE)
            Moyen   : tous les autres cas
            """
            td = str(tech_dec).upper()
            fd = str(fund_dec).upper()
            # Convergence forte
            if td == fd and td != 'NEUTRE' and rec_score >= 4:
                return 'Élevée'
            # Divergence directe ACHAT ↔ VENTE
            if (('ACHAT' in td and 'VENTE' in fd) or
                ('VENTE' in td and 'ACHAT' in fd)):
                return 'Faible'
            # Score extrême même sans convergence parfaite
            if rec_score == 5:
                return 'Élevée'
            if rec_score == 1:
                return 'Faible'
            return 'Moyen'

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
            raw_summaries = row.get('fundamental_summaries')
            nb_rapports_db = int(row.get('nb_rapports_fondamentaux', 0)) if pd.notna(row.get('nb_rapports_fondamentaux', None)) else 0
            
            if raw_summaries and pd.notna(raw_summaries) and str(raw_summaries).strip():
                reports = str(raw_summaries).split('###SEP_REPORT###')
                fundamental_parts = []
                for report in reports:
                    report = report.strip()
                    if not report:
                        continue
                    parts = report.split('###SEP_FIELD###')
                    if len(parts) >= 3:
                        title           = parts[0].strip()
                        report_date_str = parts[1].strip()
                        summary         = '###SEP_FIELD###'.join(parts[2:]).strip()
                        if not summary:
                            continue
                        try:
                            from datetime import date as _date
                            report_year = int(report_date_str[:4])
                            report_month = int(report_date_str[5:7]) if len(report_date_str) >= 7 else 1
                            today = _date.today()
                            months_ago = (today.year - report_year) * 12 + (today.month - report_month)
                            if months_ago <= 3:
                                freshness = "⭐ RÉCENT"
                            elif months_ago <= 12:
                                freshness = "📅 À JOUR"
                            else:
                                freshness = "⚠️ ANCIEN"
                            date_label = f"{report_date_str} {freshness}"
                        except Exception:
                            date_label = report_date_str
                        fundamental_parts.append(
                            f"--- RAPPORT: {title} | Date: {date_label} ---\n{summary}"
                        )
                    elif len(parts) == 1 and parts[0]:
                        fundamental_parts.append(parts[0])
                
                if fundamental_parts:
                    fundamental_text = "\n\n".join(fundamental_parts)
                    logging.info(f"   📄 {symbol}: {len(fundamental_parts)}/{nb_rapports_db} rapport(s) parsé(s) | {len(fundamental_text)} caractères")
                else:
                    logging.warning(f"   ⚠️ {symbol}: fundamental_summaries présent ({nb_rapports_db} en DB) mais parsing échoué")
                    logging.warning(f"   🔍 {symbol}: début={str(raw_summaries)[:200]}")
            else:
                if nb_rapports_db > 0:
                    logging.warning(f"   ⚠️ {symbol}: {nb_rapports_db} rapport(s) en DB mais fundamental_summaries vide — vérifier la requête SQL")
                else:
                    logging.info(f"   ℹ️ {symbol}: Aucune analyse fondamentale en base")

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
                'fundamental_analyses': fundamental_text if fundamental_text else "Aucun rapport financier enregistré en base pour cette société.",
                'predictions': [],
                'brvm_docs_raw': []
            }

            # ── Enrichissement avec brvm_documents ─────────────────────────────
            symbol_brvm_docs = brvm_docs_by_symbol.get(symbol, [])
            brvm_docs_text = self._format_brvm_documents_for_ai(symbol_brvm_docs)
            if brvm_docs_text:
                nb_docs = len(symbol_brvm_docs)
                logging.info(f'   📎 {symbol}: {nb_docs} document(s) brvm_documents disponibles')
                separator = '\n\n' + '─'*60 + '\n📎 DOCUMENTS OFFICIELS BRVM (brvm_documents):\n' + '─'*60 + '\n'
                existing = data_dict['fundamental_analyses']
                if existing and 'Aucun rapport' not in existing:
                    data_dict['fundamental_analyses'] = existing + separator + brvm_docs_text
                else:
                    data_dict['fundamental_analyses'] = separator.lstrip() + brvm_docs_text
            data_dict['brvm_docs_raw'] = symbol_brvm_docs

            # ── Enrichissement avec brvm_rapports_societes ──────────────────────
            symbol_rapports = brvm_rapports_by_symbol.get(symbol, [])
            rapports_text   = self._format_rapports_societes_for_ai(symbol_rapports)
            if rapports_text:
                nb_rap = len(symbol_rapports)
                logging.info(f'   📋 {symbol}: {nb_rap} rapport(s) brvm_rapports_societes disponibles')
                sep2 = ('\n\n' + '─'*60 +
                        '\n📋 RAPPORTS & COMMUNIQUÉS (brvm_rapports_societes):\n' +
                        '─'*60 + '\n')
                existing2 = data_dict['fundamental_analyses']
                if existing2 and 'Aucun rapport' not in existing2:
                    data_dict['fundamental_analyses'] = existing2 + sep2 + rapports_text
                else:
                    data_dict['fundamental_analyses'] = sep2.lstrip() + rapports_text
            data_dict['brvm_rapports_raw'] = symbol_rapports

            symbol_predictions = predictions_df[predictions_df['symbol'] == symbol]
            if not symbol_predictions.empty:
                # Récupérer prédictions complètes avec IC
                data_dict['predictions'] = [
                    {
                        'date':       str(r['prediction_date']),
                        'price':      float(r['predicted_price'])      if pd.notna(r['predicted_price'])  else None,
                        'lower':      float(r['lower_bound'])          if pd.notna(r.get('lower_bound'))  else None,
                        'upper':      float(r['upper_bound'])          if pd.notna(r.get('upper_bound'))  else None,
                        'confidence': str(r.get('confidence_level','')) if pd.notna(r.get('confidence_level')) else '',
                    }
                    for _, r in symbol_predictions.head(10).iterrows()
                    if pd.notna(r['predicted_price'])
                ]
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
            
            # ── Décision technique préliminaire (avant appel recommendation) ──
            _tech_sigs_pre = []
            for _k in ['mm_decision','bollinger_decision','macd_decision',
                       'rsi_decision','stochastic_decision']:
                _v = row.get(_k)
                if _v and pd.notna(_v):
                    _tech_sigs_pre.append(str(_v))
            _buy_pre  = sum(1 for s in _tech_sigs_pre if 'Achat' in s)
            _sell_pre = sum(1 for s in _tech_sigs_pre if 'Vente' in s)
            _td_pre   = 'ACHAT' if _buy_pre > _sell_pre else ('VENTE' if _sell_pre > _buy_pre else 'NEUTRE')

            # ── Décision fondamentale préliminaire (depuis texte IA brut) ──────
            _al_pre = analysis.lower()
            if 'achat' in _al_pre:   _fd_pre = 'ACHAT'
            elif 'vente' in _al_pre: _fd_pre = 'VENTE'
            else:                    _fd_pre = 'NEUTRE'

            recommendation, rec_score = self._extract_recommendation_from_analysis(
                analysis, tech_decision=_td_pre, fund_decision=_fd_pre
            )
            
            # ✅ V30: Calcul de la décision technique (majorité des indicateurs)
            tech_signals = []
            for key in ['mm_decision', 'bollinger_decision', 'macd_decision', 'rsi_decision', 'stochastic_decision']:
                val = row.get(key)
                if val and pd.notna(val):
                    tech_signals.append(str(val))
            
            buy_count = sum(1 for s in tech_signals if 'Achat' in s)
            sell_count = sum(1 for s in tech_signals if 'Vente' in s)
            
            if buy_count > sell_count:
                technical_decision = "ACHAT"
            elif sell_count > buy_count:
                technical_decision = "VENTE"
            else:
                technical_decision = "NEUTRE"
            
            # ✅ V30: Décision fondamentale (basée sur recommandation finale)
            if 'ACHAT' in recommendation.upper():
                fundamental_decision = "ACHAT"
            elif 'VENTE' in recommendation.upper():
                fundamental_decision = "VENTE"
            else:
                fundamental_decision = "NEUTRE"
            
            # ✅ V30: Calcul du risque chiffré
            # Créer un dict temporaire pour _calculate_risk_score
            temp_data = {
                'company_id': company_id,
                'mm_decision': row.get('mm_decision'),
                'bollinger_decision': row.get('bollinger_decision'),
                'macd_decision': row.get('macd_decision'),
                'rsi_decision': row.get('rsi_decision'),
                'stochastic_decision': row.get('stochastic_decision')
            }
            
            risk_data = self._calculate_risk_score(temp_data)
            
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
                'confidence_level': _dynamic_confidence(technical_decision, fundamental_decision, rec_score),
                'risk_level': risk_data['level'],  # ✅ V30: Niveau de risque calculé
                'investment_horizon': 'Moyen terme',
                # ✅ V30: Nouvelles colonnes
                'technical_decision': technical_decision,
                'fundamental_decision': fundamental_decision,
                'risk_score': risk_data['score'],
                'risk_details': json.dumps(risk_data['details'], ensure_ascii=False),
                # ✅ Sources documentaires enrichies
                'brvm_docs_raw':     data_dict.get('brvm_docs_raw', []),
                'brvm_rapports_raw': data_dict.get('brvm_rapports_raw', []),
                'predictions_full':  data_dict.get('predictions', []),
            }
            # ── Score composite d'investissement ────────────────────────────────
            inv_score, inv_label = self._compute_investment_score(all_company_data[symbol])
            all_company_data[symbol]['investment_score'] = inv_score
            all_company_data[symbol]['investment_label'] = inv_label
        
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
