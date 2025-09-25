# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER (V2.0 - POSTGRESQL)
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

# --- Récupération des Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

class BRVMAnalyzer:
    def __init__(self):
        self.societes_mapping = {
            'NTLC': {'nom_rapport': 'NESTLE CI', 'alternatives': ['nestle ci', 'nestle']},
            'PALC': {'nom_rapport': 'PALM CI', 'alternatives': ['palm ci']},
            'TTLC': {'nom_rapport': 'TOTALENERGIES MARKETING CI', 'alternatives': ['totalenergies marketing ci', 'total ci']},
            'TTLS': {'nom_rapport': 'TOTALENERGIES MARKETING SN', 'alternatives': ['totalenergies marketing senegal', 'total senegal']},
            'ECOC': {'nom_rapport': 'ECOBANK COTE D\'IVOIRE', 'alternatives': ['ecobank cote d ivoire', 'ecobank ci']},
            'NSBC': {'nom_rapport': 'NSIA BANQUE CI', 'alternatives': ['nsia banque ci', 'nsbc']},
            'SGBC': {'nom_rapport': 'SOCIETE GENERALE CI', 'alternatives': ['societe generale ci', 'sgb ci']},
            'ONTBF': {'nom_rapport': 'ONATEL BF', 'alternatives': ['onatel bf', 'moov africa']},
            'ORAC': {'nom_rapport': 'ORANGE COTE D\'IVOIRE', 'alternatives': ['orange ci', "orange cote d ivoire"]},
            'SNTS': {'nom_rapport': 'SONATEL SN', 'alternatives': ['sonatel sn', 'fctc sonatel', 'sonatel']},
            'SCRC': {'nom_rapport': 'SUCRIVOIRE', 'alternatives': ['sucrivoire']},
            'SICC': {'nom_rapport': 'SICOR CI', 'alternatives': ['sicor ci', 'sicor']},
            'SLBC': {'nom_rapport': 'SOLIBRA CI', 'alternatives': ['solibra ci', 'solibra']},
            'SOGC': {'nom_rapport': 'SOGB CI', 'alternatives': ['sogb ci', 'sogb']},
            'SPHC': {'nom_rapport': 'SAPH CI', 'alternatives': ['saph ci', 'saph']},
            'STBC': {'nom_rapport': 'SITAB CI', 'alternatives': ['sitab ci', 'sitab']},
            'UNLC': {'nom_rapport': 'UNILEVER CI', 'alternatives': ['unilever ci']},
            'ABJC': {'nom_rapport': 'SERVAIR ABIDJAN CI', 'alternatives': ['servair abidjan ci', 'servair']},
            'BNBC': {'nom_rapport': 'BERNABE CI', 'alternatives': ['bernabe ci']},
            'CFAC': {'nom_rapport': 'CFAO MOTORS CI', 'alternatives': ['cfao motors ci']},
            'LNBB': {'nom_rapport': 'LOTERIE NATIONALE BN', 'alternatives': ['loterie nationale bn', 'lonab']},
            'NEIC': {'nom_rapport': 'NEI-CEDA CI', 'alternatives': ['nei-ceda ci']},
            'PRSC': {'nom_rapport': 'TRACTAFRIC MOTORS CI', 'alternatives': ['tractafric motors ci', 'tractafric']},
            'UNXC': {'nom_rapport': 'UNIWAX CI', 'alternatives': ['uniwax ci']},
            'SHEC': {'nom_rapport': 'VIVO ENERGY CI', 'alternatives': ['vivo energy ci']},
            'SMBC': {'nom_rapport': 'SMB CI', 'alternatives': ['smb ci']},
            'BICB': {'nom_rapport': 'BICI BN', 'alternatives': ['bici bn', 'bicib']},
            'BICC': {'nom_rapport': 'BICI CI', 'alternatives': ['bici ci']},
            'BOAB': {'nom_rapport': 'BANK OF AFRICA BN', 'alternatives': ['bank of africa bn']},
            'BOABF': {'nom_rapport': 'BANK OF AFRICA BF', 'alternatives': ['bank of africa bf']},
            'BOAC': {'nom_rapport': 'BANK OF AFRICA CI', 'alternatives': ['bank of africa ci']},
            'BOAM': {'nom_rapport': 'BANK OF AFRICA ML', 'alternatives': ['bank of africa ml']},
            'BOAN': {'nom_rapport': 'BANK OF AFRICA NG', 'alternatives': ['bank of africa ng']},
            'BOAS': {'nom_rapport': 'BANK OF AFRICA SN', 'alternatives': ['bank of africa sn']},
            'CBIBF': {'nom_rapport': 'CORIS BANKING INTERNATIONAL', 'alternatives': ['coris bank international', 'coris bank']},
            'ETIT': {'nom_rapport': 'ECOBANK TRANSNATIONAL INCORPORATED', 'alternatives': ['ecobank trans', 'ecobank tg']},
            'ORGT': {'nom_rapport': 'ORAGROUP TOGO', 'alternatives': ['oragroup tg', 'oragroup']},
            'SAFC': {'nom_rapport': 'SAFCA CI', 'alternatives': ['safca ci']},
            'SIBC': {'nom_rapport': 'SOCIETE IVOIRIENNE DE BANQUE', 'alternatives': ['societe ivoirienne de banque', 'sib']},
            'CABC': {'nom_rapport': 'SICABLE CI', 'alternatives': ['sicable ci', 'sicable']},
            'FTSC': {'nom_rapport': 'FILTISAC CI', 'alternatives': ['filtisac ci']},
            'SDSC': {'nom_rapport': 'AFRICA GLOBAL LOGISTICS', 'alternatives': ['africa global logistics', 'agl']},
            'SEMC': {'nom_rapport': 'EVIOSYS PACKAGING', 'alternatives': ['eviosys packaging', 'seme']},
            'SIVC': {'nom_rapport': 'AIR LIQUIDE CI', 'alternatives': ['air liquide ci']},
            'STAC': {'nom_rapport': 'SETAO CI', 'alternatives': ['setao ci']},
            'CIEC': {'nom_rapport': 'CIE CI', 'alternatives': ['cie ci']},
            'SDCC': {'nom_rapport': 'SODE CI', 'alternatives': ['sode ci', 'sode']},
        }
        self.db_conn = None
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
            self.db_conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            logging.info("✅ Connexion DB pour analyse fondamentale réussie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur de connexion DB: {e}")
            return False

    def _load_analysis_memory_from_db(self):
        logging.info("Chargement de la mémoire d'analyse depuis PostgreSQL...")
        try:
            cur = self.db_conn.cursor()
            cur.execute("SELECT report_url FROM fundamental_analysis;")
            self.analysis_memory = {row[0] for row in cur.fetchall()}
            cur.close()
            logging.info(f"{len(self.analysis_memory)} analyses pré-existantes chargées.")
        except Exception as e:
            logging.error(f"❌ Impossible de charger la mémoire d'analyse: {e}")

    def _save_to_memory_db(self, company_id, report, summary):
        try:
            cur = self.db_conn.cursor()
            cur.execute("""
                INSERT INTO fundamental_analysis (company_id, report_url, report_title, report_date, analysis_summary)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (report_url) DO NOTHING;
            """, (company_id, report['url'], report['titre'], report['date'], summary))
            self.db_conn.commit()
            cur.close()
            self.analysis_memory.add(report['url']) # Mettre à jour la mémoire de session
            logging.info(f"    -> Analyse pour {os.path.basename(report['url'])} sauvegardée en DB.")
        except Exception as e:
            logging.error(f"    -> ERREUR lors de la sauvegarde en DB : {e}")
            self.db_conn.rollback()

    def _analyze_pdf_with_gemini(self, company_id, symbol, report):
        pdf_url = report['url']
        if pdf_url in self.analysis_memory:
            return

        max_retries = len(self.api_keys)
        for attempt in range(max_retries):
            temp_pdf_path = "temp_report.pdf"
            uploaded_file = None
            try:
                logging.info(f"    -> Nouvelle analyse IA (clé #{self.current_key_index + 1}) : {os.path.basename(pdf_url)}")
                response = self.session.get(pdf_url, timeout=45, verify=False)
                response.raise_for_status()
                with open(temp_pdf_path, 'wb') as f:
                    f.write(response.content)
                
                uploaded_file = genai.upload_file(path=temp_pdf_path, display_name="Rapport Financier")
                
                prompt = "..." # Le prompt reste le même
                
                response = self.gemini_model.generate_content([prompt, uploaded_file])
                
                analysis_text = response.text if hasattr(response, 'text') else "Analyse non générée."

                if "erreur" not in analysis_text.lower():
                    self._save_to_memory_db(company_id, report, analysis_text)
                    self.newly_analyzed_reports.append(f"Rapport pour {symbol}:\n{analysis_text}\n")
                
                return
            except api_exceptions.ResourceExhausted as e:
                logging.warning(f"Quota atteint pour la clé API #{self.current_key_index + 1}.")
                if not self._rotate_api_key(): return
            except Exception as e:
                logging.error(f"    -> Erreur technique inattendue lors de l'analyse IA : {e}")
                return
            finally:
                if uploaded_file:
                    try: genai.delete_file(uploaded_file.name)
                    except: pass
                if os.path.exists(temp_pdf_path): os.remove(temp_pdf_path)

    # ... (les autres fonctions comme setup_selenium, _configure_gemini_with_rotation, _normalize_text, etc. restent les mêmes)

    def run_and_get_results(self):
        logging.info("="*60)
        logging.info("ÉTAPE 3 : DÉMARRAGE DE L'ANALYSE FONDAMENTALE (VERSION POSTGRESQL)")
        logging.info("="*60)
        
        if not self.connect_to_db() or not self._configure_gemini_with_rotation():
            if self.db_conn: self.db_conn.close()
            return {}, []
        
        try:
            self._load_analysis_memory_from_db()
            self.setup_selenium()
            if not self.driver: return {}, []

            cur = self.db_conn.cursor()
            cur.execute("SELECT symbol, id, name FROM companies")
            companies_from_db = cur.fetchall()
            cur.close()
            
            self.company_ids = {symbol: (id, name) for symbol, id, name in companies_from_db}

            all_reports = self._find_all_reports()
            
            for symbol, (company_id, company_name) in self.company_ids.items():
                logging.info(f"\n📊 Traitement des rapports pour {symbol} - {company_name}")
                company_reports = all_reports.get(symbol, [])
                if not company_reports:
                    logging.info(f"  -> Aucun rapport trouvé sur le site pour {symbol}.")
                    continue

                for report in company_reports:
                    self._analyze_pdf_with_gemini(company_id, symbol, report)
                    time.sleep(1) # Petite pause

            logging.info("\n✅ Traitement de toutes les sociétés terminé.")
            
            # Récupérer toutes les analyses de la base de données pour le rapport final
            cur = self.db_conn.cursor()
            cur.execute("SELECT c.symbol, fa.analysis_summary FROM fundamental_analysis fa JOIN companies c ON fa.company_id = c.id;")
            final_results = defaultdict(lambda: {'rapports_analyses': []})
            for symbol, summary in cur.fetchall():
                final_results[symbol]['rapports_analyses'].append({'analyse_ia': summary})
            cur.close()
            
            return (dict(final_results), self.newly_analyzed_reports)
            
        except Exception as e:
            logging.critical(f"❌ Erreur critique : {e}", exc_info=True)
            return {}, []
        finally:
            if self.driver: self.driver.quit()
            if self.db_conn: self.db_conn.close()

# ... (les fonctions de scraping _find_all_reports, _get_symbol_from_name, etc. restent les mêmes)
