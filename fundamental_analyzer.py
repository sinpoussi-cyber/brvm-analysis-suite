# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER V7.4 - CORRECTION API GEMINI
# ==============================================================================

import requests
from bs4 import BeautifulSoup
import time
import re
import os
from datetime import datetime
import logging
import unicodedata
import urllib3
import base64
from collections import defaultdict
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# ✅ CONFIGURATION GEMINI CORRIGÉE
GEMINI_MODEL = "gemini-1.5-flash"
# Utiliser v1beta qui est plus stable et largement supporté
GEMINI_API_VERSION = "v1beta"
REQUESTS_PER_MINUTE_LIMIT = 15

class BRVMAnalyzer:
    def __init__(self):
        self.societes_mapping = {
            'NTLC': {'nom_rapport': 'NESTLE CI', 'alternatives': ['nestle ci', 'nestle']},
            'PALC': {'nom_rapport': 'PALM CI', 'alternatives': ['palm ci']},
            # ... (garder tous les autres mappings)
        }
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        self.api_keys = []
        self.current_key_index = 0
        self.request_timestamps = []

    def connect_to_db(self):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, 
                host=DB_HOST, port=DB_PORT
            )
            return conn
        except Exception as e:
            logging.error(f"❌ Erreur connexion DB: {e}")
            return None

    def _load_analysis_memory_from_db(self):
        """Charge la mémoire depuis PostgreSQL"""
        logging.info("📂 Chargement mémoire depuis PostgreSQL...")
        conn = self.connect_to_db()
        if not conn: 
            logging.error("❌ Impossible de charger la mémoire: connexion DB échouée")
            return
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'fundamental_analysis'
                    );
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    logging.warning("⚠️  Table 'fundamental_analysis' n'existe pas encore")
                    self.analysis_memory = set()
                    return
                
                cur.execute("SELECT report_url FROM fundamental_analysis;")
                urls = cur.fetchall()
                self.analysis_memory = {row[0] for row in urls}
            
            logging.info(f"   ✅ {len(self.analysis_memory)} analyse(s) chargée(s) depuis DB")
                    
        except Exception as e:
            logging.error(f"❌ Erreur chargement mémoire DB: {e}")
            self.analysis_memory = set()
        finally:
            if conn: 
                conn.close()

    def _save_to_db(self, company_id, report, summary):
        """Sauvegarde dans PostgreSQL"""
        conn = self.connect_to_db()
        if not conn: 
            logging.error("❌ Impossible de sauvegarder: connexion DB échouée")
            return False
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fundamental_analysis (company_id, report_url, report_title, report_date, analysis_summary)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (report_url) DO UPDATE SET
                        analysis_summary = EXCLUDED.analysis_summary,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                """, (company_id, report['url'], report['titre'], report['date'], summary))
                
                inserted_id = cur.fetchone()[0]
                conn.commit()
            
            self.analysis_memory.add(report['url'])
            logging.info(f"    ✅ Sauvegardé dans PostgreSQL (ID: {inserted_id})")
            return True
            
        except Exception as e:
            logging.error(f"❌ Erreur sauvegarde DB: {e}")
            conn.rollback()
            return False
        finally:
            if conn: 
                conn.close()

    def _configure_api_keys(self):
        """Charge les 33 clés API"""
        for i in range(1, 34):
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                # ✅ IMPORTANT: Nettoyer la clé des espaces
                self.api_keys.append(key.strip())
        
        if not self.api_keys:
            logging.error("❌ Aucune clé API trouvée")
            return False
        
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées")
        logging.info(f"📝 Modèle: {GEMINI_MODEL} | API Version: {GEMINI_API_VERSION}")
        return True

    def _analyze_pdf_with_direct_api(self, company_id, symbol, report):
        """Analyse un PDF avec l'API Gemini - VERSION CORRIGÉE"""
        pdf_url = report['url']
        
        # Vérification mémoire
        if pdf_url in self.analysis_memory:
            logging.info(f"    ⏭️  Déjà analysé: {os.path.basename(pdf_url)}")
            return None
        
        # Double vérification DB
        conn = self.connect_to_db()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM fundamental_analysis WHERE report_url = %s;", (pdf_url,))
                    if cur.fetchone():
                        logging.info(f"    ⏭️  Déjà en base: {os.path.basename(pdf_url)}")
                        self.analysis_memory.add(pdf_url)
                        return None
            except Exception as e:
                logging.error(f"    ⚠️  Erreur vérification DB: {e}")
            finally:
                conn.close()
        
        logging.info(f"    🆕 NOUVEAU rapport à analyser: {os.path.basename(pdf_url)}")
        
        # Gestion du rate limiting
        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        if len(self.request_timestamps) >= REQUESTS_PER_MINUTE_LIMIT:
            sleep_time = 60 - (now - self.request_timestamps[0])
            logging.warning(f"⏸️  Pause rate limit: {sleep_time + 1:.1f}s")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []
        
        if self.current_key_index >= len(self.api_keys):
            logging.error("❌ Toutes les clés API épuisées")
            return None
        
        api_key = self.api_keys[self.current_key_index]
        
        # ✅ URL CORRIGÉE - Utiliser v1beta avec x-goog-api-key dans le header
        api_url = f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models/{GEMINI_MODEL}:generateContent"
        
        try:
            logging.info(f"    🤖 Analyse IA (clé #{self.current_key_index + 1})")
            
            # Télécharger le PDF
            pdf_response = self.session.get(pdf_url, timeout=45, verify=False)
            pdf_response.raise_for_status()
            pdf_data = base64.b64encode(pdf_response.content).decode('utf-8')
            
            prompt = """Tu es un analyste financier expert spécialisé dans les entreprises de la zone UEMOA cotées à la BRVM.
Analyse le document PDF ci-joint, qui est un rapport financier, et fournis une synthèse concise en français, structurée en points clés.
Concentre-toi impérativement sur les aspects suivants :
- **Évolution du Chiffre d'Affaires (CA)** : Indique la variation en pourcentage et en valeur si possible.
- **Évolution du Résultat Net (RN)** : Indique la variation et les facteurs qui l'ont influencée.
- **Politique de Dividende** : Cherche toute mention de dividende proposé, payé ou des perspectives.
- **Performance des Activités Ordinaires/d'Exploitation** : Commente l'évolution de la rentabilité opérationnelle.
- **Perspectives et Points de Vigilance** : Relève tout point crucial pour un investisseur.
Si une information n'est pas trouvée, mentionne-le clairement. Sois factuel et base tes conclusions uniquement sur le document."""
            
            # ✅ HEADERS CORRIGÉS - Utiliser x-goog-api-key
            headers = {
                "Content-Type": "application/json",
                "x-goog-api-key": api_key
            }
            
            request_body = {
                "contents": [{
                    "parts": [
                        {"text": prompt}, 
                        {"inline_data": {"mime_type": "application/pdf", "data": pdf_data}}
                    ]
                }],
                "generationConfig": {
                    "temperature": 0.7,
                    "topK": 40,
                    "topP": 0.95,
                    "maxOutputTokens": 2048,
                }
            }
            
            self.request_timestamps.append(time.time())
            
            # ✅ REQUÊTE AVEC HEADERS
            response = requests.post(api_url, headers=headers, json=request_body, timeout=120)
            
            # Gestion des erreurs
            if response.status_code == 429:
                logging.warning(f"⚠️  Quota atteint pour clé #{self.current_key_index + 1}")
                self.current_key_index += 1
                return self._analyze_pdf_with_direct_api(company_id, symbol, report)
            
            if response.status_code == 404:
                logging.error(f"❌ 404 Not Found - Vérifier l'URL de l'API et le modèle")
                logging.error(f"   URL utilisée: {api_url}")
                self.current_key_index += 1
                return self._analyze_pdf_with_direct_api(company_id, symbol, report)
            
            if response.status_code == 403:
                logging.error(f"❌ 403 Forbidden - Clé API invalide ou permissions insuffisantes")
                self.current_key_index += 1
                return self._analyze_pdf_with_direct_api(company_id, symbol, report)
            
            response.raise_for_status()
            response_json = response.json()
            
            analysis_text = response_json['candidates'][0]['content']['parts'][0]['text']
            
            if "erreur" not in analysis_text.lower():
                if self._save_to_db(company_id, report, analysis_text):
                    self.newly_analyzed_reports.append(f"Rapport pour {symbol}:\n{analysis_text}\n")
                    return True
            
            return False
        
        except Exception as e:
            logging.error(f"    ❌ Erreur clé #{self.current_key_index + 1}: {e}")
            self.current_key_index += 1
            if self.current_key_index < len(self.api_keys):
                return self._analyze_pdf_with_direct_api(company_id, symbol, report)
            return False

    # ... (garder toutes les autres méthodes: setup_selenium, _normalize_text, etc.)

    def run_and_get_results(self):
        """Fonction principale avec système de mémoire optimisé"""
        logging.info("="*80)
        logging.info("📄 ÉTAPE 4: ANALYSE FONDAMENTALE (V7.4 - API CORRIGÉE)")
        logging.info("="*80)
        
        conn = None
        try:
            if not self._configure_api_keys():
                return {}, []
            
            self._load_analysis_memory_from_db()
            logging.info(f"📊 Mémoire chargée: {len(self.analysis_memory)} rapport(s) déjà analysé(s)")
            
            self.setup_selenium()
            if not self.driver: 
                return {}, []
            
            conn = self.connect_to_db()
            if not conn: 
                return {}, []
            
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, id, name FROM companies")
                companies_from_db = cur.fetchall()
            conn.close()
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}
            
            logging.info("\n🔍 Phase 1: Collecte des rapports sur le site BRVM...")
            all_reports = self._find_all_reports()
            
            total_reports_found = sum(len(reports) for reports in all_reports.values())
            logging.info(f"\n📊 Statistiques de collecte:")
            logging.info(f"   • Total rapports trouvés: {total_reports_found}")
            logging.info(f"   • Sociétés avec rapports: {len(all_reports)}")
            
            logging.info(f"\n🤖 Phase 2: Analyse des nouveaux rapports...")
            
            total_analyzed = 0
            total_skipped = 0
            
            for symbol, (company_id, company_name) in self.company_ids.items():
                logging.info(f"\n📊 Traitement {symbol} - {company_name}")
                company_reports = all_reports.get(symbol, [])
                
                if not company_reports:
                    logging.info(f"   ⏭️  Aucun rapport pour {symbol}")
                    continue
                
                date_2024_start = datetime(2024, 1, 1).date()
                recent_reports = [r for r in company_reports if r['date'] >= date_2024_start]
                recent_reports.sort(key=lambda x: x['date'], reverse=True)
                
                logging.info(f"   📂 {len(recent_reports)} rapport(s) récent(s) trouvé(s)")
                
                already_analyzed = []
                new_reports = []
                
                for report in recent_reports:
                    if report['url'] in self.analysis_memory:
                        already_analyzed.append(report)
                    else:
                        new_reports.append(report)
                
                logging.info(f"   ✅ Déjà analysés: {len(already_analyzed)}")
                logging.info(f"   🆕 Nouveaux à analyser: {len(new_reports)}")
                
                for report in new_reports:
                    result = self._analyze_pdf_with_direct_api(company_id, symbol, report)
                    if result is True:
                        total_analyzed += 1
                    elif result is None:
                        total_skipped += 1
                
                total_skipped += len(already_analyzed)
            
            logging.info("\n✅ Traitement terminé")
            logging.info(f"📊 Nouvelles analyses effectuées: {total_analyzed}")
            logging.info(f"📊 Rapports ignorés (déjà en DB): {total_skipped}")
            logging.info(f"💾 Total dans la mémoire: {len(self.analysis_memory)} rapport(s)")
            
            conn = self.connect_to_db()
            if not conn: 
                return {}, []
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.symbol, fa.analysis_summary, c.name 
                    FROM fundamental_analysis fa 
                    JOIN companies c ON fa.company_id = c.id
                """)
                final_results = defaultdict(lambda: {'rapports_analyses': [], 'nom': ''})
                
                for symbol, summary, name in cur.fetchall():
                    final_results[symbol]['rapports_analyses'].append({'analyse_ia': summary})
                    final_results[symbol]['nom'] = name
            
            logging.info(f"📊 Résultats finaux: {len(final_results)} société(s) avec analyses")
            return (dict(final_results), self.newly_analyzed_reports)
        
        except Exception as e:
            logging.critical(f"❌ Erreur critique: {e}", exc_info=True)
            return {}, []
        
        finally:
            if self.driver: 
                self.driver.quit()
            if conn and not conn.closed: 
                conn.close()

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
