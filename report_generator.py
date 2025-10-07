# ==============================================================================
# MODULE: COMPREHENSIVE REPORT GENERATOR (V4.4 - VERSION CORRIGÉE)
# ==============================================================================

import psycopg2
import pandas as pd
import os
import json
import time
import logging
from datetime import datetime
from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
import requests
from collections import defaultdict

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Secrets de base de données
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# Configuration API
REQUESTS_PER_MINUTE_LIMIT = 10

class ComprehensiveReportGenerator:
    def __init__(self, db_conn):
        """Initialise le générateur de rapports avec une connexion DB."""
        self.db_conn = db_conn
        self.api_keys = []
        self.current_key_index = 0
        self.request_timestamps = []
    
    def _configure_api_keys(self):
        """Charge les clés API Gemini depuis les variables d'environnement."""
        for i in range(1, 20):
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                self.api_keys.append(key)
        
        if not self.api_keys:
            logging.warning("⚠️ Aucune clé API Gemini trouvée. Les analyses IA seront vides.")
            return False
        
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées.")
        return True
    
    def _call_gemini_with_retry(self, prompt):
        """Appelle l'API Gemini avec gestion des quotas et retry."""
        if not self.api_keys:
            return "Analyse IA non disponible (aucune clé API configurée)."
        
        # Gestion du rate limiting (10 requêtes/minute)
        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        if len(self.request_timestamps) >= REQUESTS_PER_MINUTE_LIMIT:
            sleep_time = 60 - (now - self.request_timestamps[0]) if self.request_timestamps else 60
            logging.warning(f"⏳ Limite de requêtes/minute atteinte. Pause de {sleep_time + 1:.1f} secondes...")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []
        
        # Essayer avec les clés disponibles
        while self.current_key_index < len(self.api_keys):
            api_key = self.api_keys[self.current_key_index]
            api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
            
            try:
                self.request_timestamps.append(time.time())
                request_body = {
                    "contents": [{
                        "parts": [{"text": prompt}]
                    }]
                }
                
                response = requests.post(api_url, json=request_body, timeout=60)
                
                # Si quota atteint, passer à la clé suivante
                if response.status_code == 429:
                    logging.warning(f"⚠️ Quota atteint pour la clé API #{self.current_key_index + 1}. Passage à la suivante...")
                    self.current_key_index += 1
                    continue
                
                response.raise_for_status()
                response_json = response.json()
                
                return response_json['candidates'][0]['content']['parts'][0]['text']
            
            except Exception as e:
                logging.error(f"❌ Erreur avec la clé #{self.current_key_index + 1}: {e}")
                self.current_key_index += 1
        
        return "Erreur d'analyse : Le quota de toutes les clés API a probablement été atteint."
    
    def _get_all_data_from_db(self):
        """Récupère toutes les données d'analyse depuis PostgreSQL."""
        logging.info("📊 Récupération de toutes les données d'analyse depuis PostgreSQL...")
        
        query = """
        WITH latest_historical_data AS (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
            FROM historical_data
        )
        SELECT
            c.symbol,
            c.name as company_name,
            lhd.trade_date,
            lhd.price,
            ta.mm_decision,
            ta.bollinger_decision,
            ta.macd_decision,
            ta.rsi_decision,
            ta.stochastic_decision,
            (SELECT STRING_AGG(fa.analysis_summary, E'\\n---\\n' ORDER BY fa.report_date DESC)
             FROM fundamental_analysis fa 
             WHERE fa.company_id = c.id) as fundamental_summaries
        FROM companies c
        LEFT JOIN latest_historical_data lhd ON c.id = lhd.company_id
        LEFT JOIN technical_analysis ta ON lhd.id = ta.historical_data_id
        WHERE lhd.rn <= 50 OR lhd.rn IS NULL
        ORDER BY c.symbol, lhd.trade_date;
        """
        
        df = pd.read_sql(query, self.db_conn)
        
        # Structurer les données par société
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
        """Génère une analyse IA de l'évolution du prix."""
        if df_prices.empty or df_prices['price'].isnull().all():
            return "Données de prix insuffisantes pour une analyse."
        
        data_string = df_prices.to_csv(index=False)
        
        prompt = f"""Analyse l'évolution du cours de cette action sur les 50 derniers jours disponibles.

Données CSV:
{data_string}

Fournis une analyse structurée qui inclut:
1. La tendance générale (haussière, baissière, ou stable)
2. Les chiffres clés (cours de début, cours de fin, variation en %, plus haut, plus bas)
3. La dynamique récente (volatilité, momentum)

Sois concis et factuel. Maximum 5-6 phrases."""
        
        return self._call_gemini_with_retry(prompt)
    
    def _analyze_technical_indicators(self, series_indicators):
        """Génère une analyse IA des indicateurs techniques."""
        data_string = series_indicators.to_string()
        
        prompt = f"""Analyse ces indicateurs techniques pour le jour le plus récent:

{data_string}

Pour chaque indicateur (Moyennes Mobiles, Bollinger, MACD, RSI, Stochastique):
- Donne une interprétation de 2-3 phrases
- Indique un signal clair: Achat, Vente, ou Neutre

Sois concis et actionnable."""
        
        return self._call_gemini_with_retry(prompt)
    
    def _summarize_fundamental_analysis(self, summaries):
        """Génère une synthèse IA des analyses fondamentales."""
        prompt = f"""Synthétise ces analyses de rapports financiers en 3 ou 4 points clés pour un investisseur.

Concentre-toi sur:
- L'évolution du chiffre d'affaires
- Le résultat net
- La politique de dividende
- Les perspectives

Analyses:
{summaries}

Sois factuel et structuré en bullet points."""
        
        return self._call_gemini_with_retry(prompt)
    
    def _create_main_report(self, company_analyses):
        """Crée le rapport de synthèse principal au format Word."""
        logging.info("📝 Création du rapport de synthèse principal...")
        
        doc = Document()
        
        # En-tête du document
        title = doc.add_heading('Rapport de Synthèse d\'Investissement - BRVM', level=0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Date de génération
        date_para = doc.add_paragraph()
        date_para.add_run(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}").italic = True
        date_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        doc.add_paragraph()  # Espace
        
        # Introduction
        intro = doc.add_paragraph()
        intro.add_run("Ce rapport présente une analyse complète de chaque société cotée à la BRVM, incluant l'évolution des cours, l'analyse technique des indicateurs, et une synthèse des données fondamentales.").bold = False
        
        doc.add_page_break()
        
        # Analyser chaque société
        for symbol in sorted(company_analyses.keys()):
            analyses = company_analyses[symbol]
            nom_societe = analyses.get('nom_societe', symbol)
            
            # Titre de la société
            company_heading = doc.add_heading(f'{nom_societe} ({symbol})', level=1)
            
            # 1. Évolution du cours
            doc.add_heading('1. Évolution du Cours (50 derniers jours)', level=2)
            doc.add_paragraph(analyses.get('price_analysis', 'Analyse du prix non disponible.'))
            
            # 2. Analyse technique
            doc.add_heading('2. Analyse Technique des Indicateurs', level=2)
            doc.add_paragraph(analyses.get('technical_analysis', 'Analyse technique non disponible.'))
            
            # 3. Synthèse fondamentale
            doc.add_heading('3. Synthèse Fondamentale', level=2)
            doc.add_paragraph(analyses.get('fundamental_summary', 'Analyse fondamentale non disponible.'))
            
            doc.add_page_break()
        
        # Sauvegarder le document
        output_filename = f"Rapport_Synthese_Investissement_BRVM_{time.strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_filename)
        
        logging.info(f"🎉 Rapport de synthèse principal généré : {output_filename}")
        return output_filename
    
    def generate_all_reports(self, new_fundamental_analyses):
        """Génère tous les rapports d'analyse."""
        logging.info("=" * 60)
        logging.info("ÉTAPE 4 : DÉMARRAGE DE LA GÉNÉRATION DES RAPPORTS")
        logging.info("=" * 60)
        
        # Configurer les clés API
        if not self._configure_api_keys():
            logging.warning("⚠️ Génération de rapports ignorée (pas de clés API).")
            return
        
        # Récupérer toutes les données
        all_data = self._get_all_data_from_db()
        
        if not all_data:
            logging.warning("⚠️ Aucune donnée à analyser. Rapports non générés.")
            return
        
        company_analyses = {}
        total_companies = len(all_data)
        
        # Générer les analyses IA pour chaque société
        for idx, (symbol, data) in enumerate(all_data.items(), 1):
            logging.info(f"--- [{idx}/{total_companies}] Génération des synthèses IA pour : {symbol} ---")
            
            company_analyses[symbol] = {
                'nom_societe': data['nom_societe'],
                'price_analysis': self._analyze_price_evolution(data['price_data']),
                'technical_analysis': self._analyze_technical_indicators(data['indicator_data']),
                'fundamental_summary': self._summarize_fundamental_analysis(data['fundamental_summaries'])
            }
            
            # Petit délai entre chaque société
            time.sleep(1)
        
        # Créer le rapport principal
        report_filename = self._create_main_report(company_analyses)
        
        # Résumé
        logging.info("\n" + "=" * 60)
        logging.info("📊 RÉSUMÉ DE LA GÉNÉRATION DE RAPPORTS")
        logging.info("=" * 60)
        logging.info(f"   • Sociétés analysées : {len(company_analyses)}")
        logging.info(f"   • Rapport généré : {report_filename}")
        logging.info(f"   • Clés API utilisées : {self.current_key_index + 1}/{len(self.api_keys)}")
        logging.info("=" * 60)
        logging.info("✅ Génération de rapports terminée avec succès")

if __name__ == "__main__":
    db_conn = None
    try:
        # Vérifier les secrets
        if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
            logging.error("❌ Des secrets de base de données sont manquants. Arrêt du script.")
        else:
            # Connexion à la base
            db_conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            
            # Générer les rapports
            report_generator = ComprehensiveReportGenerator(db_conn)
            report_generator.generate_all_reports([])
    
    except Exception as e:
        logging.error(f"❌ Erreur fatale dans le générateur de rapports : {e}", exc_info=True)
    
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
