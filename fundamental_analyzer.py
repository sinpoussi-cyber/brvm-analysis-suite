# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER (V3.4 - GESTION ROBUSTE DE LA CONNEXION DB)
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
import json
from collections import defaultdict
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2
from psycopg2 import sql
import google.generativeai as genai
from google.api_core import exceptions as api_exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration & Secrets ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

class BRVMAnalyzer:
    def __init__(self):
        self.societes_mapping = {
            # ... (votre mapping de sociétés reste ici) ...
        }
        self.driver = None
        self.gemini_model = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        self.api_keys = []
        self.current_key_index = 0

    def connect_to_db(self):
        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            return conn
        except Exception as e:
            logging.error(f"❌ Erreur de connexion DB: {e}")
            return None

    def _load_analysis_memory_from_db(self):
        logging.info("Chargement de la mémoire d'analyse depuis PostgreSQL...")
        conn = self.connect_to_db()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT report_url FROM fundamental_analysis;")
                self.analysis_memory = {row[0] for row in cur.fetchall()}
            logging.info(f"{len(self.analysis_memory)} analyses pré-existantes chargées.")
        except Exception as e:
            logging.error(f"❌ Impossible de charger la mémoire d'analyse: {e}")
        finally:
            if conn: conn.close()

    def _save_to_memory_db(self, company_id, report, summary):
        conn = self.connect_to_db()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fundamental_analysis (company_id, report_url, report_title, report_date, analysis_summary)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (report_url) DO NOTHING;
                """, (company_id, report['url'], report['titre'], report['date'], summary))
                conn.commit()
            self.analysis_memory.add(report['url'])
            logging.info(f"    -> Analyse pour {os.path.basename(report['url'])} sauvegardée en DB.")
        except Exception as e:
            logging.error(f"    -> ERREUR lors de la sauvegarde en DB : {e}")
            conn.rollback()
        finally:
            if conn: conn.close()
    
    # ... (le reste du code est identique jusqu'à run_and_get_results)
    
    def run_and_get_results(self):
        logging.info("="*60)
        logging.info("ÉTAPE 3 : DÉMARRAGE DE L'ANALYSE FONDAMENTALE (VERSION POSTGRESQL)")
        logging.info("="*60)
        
        if not self._configure_gemini_with_rotation():
            return {}, []
        
        try:
            self._load_analysis_memory_from_db()
            self.setup_selenium()
            if not self.driver: return {}, []

            conn = self.connect_to_db()
            if not conn: return {}, []
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, id, name FROM companies")
                companies_from_db = cur.fetchall()
            conn.close() # Fermer la connexion après avoir récupéré les infos initiales
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}

            all_reports = self._find_all_reports()
            
            for symbol, (company_id, company_name) in self.company_ids.items():
                # ... (boucle de traitement identique)

            logging.info("\n✅ Traitement de toutes les sociétés terminé.")
            
            # Reconnecter pour la lecture finale
            conn = self.connect_to_db()
            if not conn: return {}, []
            with conn.cursor() as cur:
                cur.execute("SELECT c.symbol, fa.analysis_summary, c.name FROM fundamental_analysis fa JOIN companies c ON fa.company_id = c.id;")
                final_results = defaultdict(lambda: {'rapports_analyses': [], 'nom': ''})
                for symbol, summary, name in cur.fetchall():
                    final_results[symbol]['rapports_analyses'].append({'analyse_ia': summary})
                    final_results[symbol]['nom'] = name
            
            return (dict(final_results), self.newly_analyzed_reports)
            
        except Exception as e:
            logging.critical(f"❌ Erreur critique : {e}", exc_info=True)
            return {}, []
        finally:
            if self.driver: self.driver.quit()
            if conn: conn.close()

# ... (le reste du fichier est identique)

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
