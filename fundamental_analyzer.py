# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER (V6.0 - GEMINI 2.5 PRO)
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
import base64

# Import du gestionnaire de synchronisation
from sync_data_manager import SyncDataManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

REQUESTS_PER_MINUTE_LIMIT = 10

class BRVMAnalyzer:
    def __init__(self):
        self.societes_mapping = {
            # ... (mapping complet, identique à avant)
        }
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json'
        })
        self.analysis_memory = set()
        self.company_ids = {}
        self.newly_analyzed_reports = []
        self.api_keys = []
        self.current_key_index = 0
        self.request_timestamps = []
        self.sync_manager = SyncDataManager()

    def _configure_api_keys(self):
        for i in range(1, 20): 
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key: self.api_keys.append(key)
        if not self.api_keys:
            logging.error("❌ Aucune clé API trouvée.")
            return False
        logging.info(f"✅ {len(self.api_keys)} clé(s) API Gemini chargées.")
        return True

    def _analyze_pdf_with_direct_api(self, company_id, symbol, report):
        pdf_url = report['url']
        if pdf_url in self.analysis_memory:
            return

        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        if len(self.request_timestamps) >= REQUESTS_PER_MINUTE_LIMIT:
            sleep_time = 60 - (now - self.request_timestamps[0])
            logging.warning(f"Limite de requêtes/minute atteinte. Pause de {sleep_time + 1:.1f} secondes...")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []

        if self.current_key_index >= len(self.api_keys):
            logging.error("Toutes les clés API ont été épuisées. Arrêt des analyses.")
            return

        api_key = self.api_keys[self.current_key_index]
        
        # ✅ UTILISATION DE GEMINI 2.5 PRO
        api_url = f"https://generativelanguage.googleapis.com/v1/models/gemini-2.5-pro:generateContent?key={api_key}"
        
        # Alternative si gemini-2.5-pro ne fonctionne pas :
        # api_url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-pro:generateContent?key={api_key}"
        
        try:
            logging.info(f"    -> Nouvelle analyse IA Gemini 2.5 Pro (clé #{self.current_key_index + 1}) : {os.path.basename(pdf_url)}")
            
            # Télécharger le PDF
            pdf_response = self.session.get(pdf_url, timeout=45, verify=False)
            pdf_response.raise_for_status()
            pdf_data = base64.b64encode(pdf_response.content).decode('utf-8')

            # Prompt optimisé pour Gemini 2.5 Pro
            prompt = """
Tu es un analyste financier expert spécialisé dans les entreprises de la zone UEMOA cotées à la BRVM.

**MISSION** : Analyse le document PDF ci-joint (rapport financier) et fournis une synthèse concise en français, structurée en points clés.

**STRUCTURE DE L'ANALYSE** :

1. **📊 Chiffre d'Affaires (CA)**
   - Montant et variation (%, valeur)
   - Facteurs explicatifs de l'évolution

2. **💰 Résultat Net (RN)**
   - Montant et variation
   - Facteurs d'influence

3. **💵 Dividende**
   - Montant proposé/versé
   - Rendement si calculable
   - Si non mentionné, indiquer : "Politique de dividende non mentionnée"

4. **🏭 Performance Opérationnelle**
   - Rentabilité d'exploitation
   - Marges (si disponibles)

5. **🔮 Perspectives & Vigilance**
   - Points forts à noter
   - Risques/vigilances
   - Investissements majeurs prévus

**CONSIGNES** :
- Sois factuel et précis
- Utilise UNIQUEMENT les données du document
- Si une info manque, le mentionner clairement
- Privilégie les chiffres et pourcentages
- Synthèse maximale : 250-300 mots

Commence ton analyse maintenant.
"""

            # Format de requête optimisé
            request_body = {
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {
                            "inline_data": {
                                "mime_type": "application/pdf",
                                "data": pdf_data
                            }
                        }
                    ]
                }],
                "generationConfig": {
                    "temperature": 0.3,  # Moins créatif, plus factuel
                    "topK": 40,
                    "topP": 0.95,
                    "maxOutputTokens": 1024,  # ~300 mots en français
                }
            }

            self.request_timestamps.append(time.time())
            
            headers = {
                "Content-Type": "application/json"
            }
            
            response = requests.post(api_url, json=request_body, headers=headers, timeout=120)

            # Gestion spécifique des erreurs
            if response.status_code == 404:
                logging.warning(f"    -> ⚠️ Gemini 2.5 Pro non disponible avec cette clé. Tentative avec Gemini 1.5 Pro...")
                # Fallback vers Gemini 1.5 Pro
                api_url_fallback = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-pro:generateContent?key={api_key}"
                response = requests.post(api_url_fallback, json=request_body, headers=headers, timeout=120)
                
                if response.status_code == 404:
                    logging.error(f"    -> ❌ Gemini 1.5 Pro non plus disponible. Passage à la clé suivante.")
                    self.current_key_index += 1
                    if self.current_key_index < len(self.api_keys):
                        self._analyze_pdf_with_direct_api(company_id, symbol, report)
                    return
            
            if response.status_code == 429:
                logging.warning(f"Quota atteint pour la clé API #{self.current_key_index + 1}. Passage à la suivante.")
                self.current_key_index += 1
                if self.current_key_index < len(self.api_keys):
                    self._analyze_pdf_with_direct_api(company_id, symbol, report)
                return

            response.raise_for_status()
            response_json = response.json()
            
            # Extraction de la réponse
            try:
                analysis_text = response_json['candidates'][0]['content']['parts'][0]['text']
            except (KeyError, IndexError) as e:
                logging.error(f"    -> Erreur de parsing de la réponse: {e}")
                # Log de la réponse pour debugging
                logging.debug(f"    -> Réponse complète: {json.dumps(response_json, indent=2)[:500]}")
                return

            if "erreur" not in analysis_text.lower() and len(analysis_text) > 50:
                # SYNCHRONISATION AUTOMATIQUE
                self.sync_manager.sync_fundamental_analysis(
                    company_id=company_id,
                    symbol=symbol,
                    report_url=report['url'],
                    report_title=report['titre'],
                    report_date=report['date'],
                    analysis_summary=analysis_text
                )
                
                self.analysis_memory.add(pdf_url)
                self.newly_analyzed_reports.append(f"Rapport pour {symbol}:\n{analysis_text}\n")
                logging.info(f"    -> ✅ Analyse IA complétée ({len(analysis_text)} caractères)")
            else:
                logging.warning(f"    -> ⚠️ Analyse trop courte ou contient des erreurs")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.error(f"    -> ❌ ERREUR 404: Modèle Gemini non accessible")
                logging.error(f"    -> Vérifiez vos clés sur https://aistudio.google.com/app/apikey")
            elif e.response.status_code == 400:
                logging.error(f"    -> ❌ ERREUR 400: Requête invalide")
                logging.error(f"    -> Réponse: {e.response.text[:200]}")
            else:
                logging.error(f"    -> Erreur HTTP {e.response.status_code}: {e}")
            
            self.current_key_index += 1
            if self.current_key_index < len(self.api_keys):
                self._analyze_pdf_with_direct_api(company_id, symbol, report)
                
        except requests.exceptions.Timeout:
            logging.error(f"    -> ⏱️ Timeout lors de l'appel API (>120s)")
            self.current_key_index += 1
            if self.current_key_index < len(self.api_keys):
                self._analyze_pdf_with_direct_api(company_id, symbol, report)
                
        except Exception as e:
            logging.error(f"    -> Erreur technique avec la clé #{self.current_key_index + 1} : {e}")
            self.current_key_index += 1
            if self.current_key_index < len(self.api_keys):
                self._analyze_pdf_with_direct_api(company_id, symbol, report)

    # ... (reste des méthodes identique : setup_selenium, _normalize_text, etc.)
    # ... (méthodes _find_all_reports, run_and_get_results restent inchangées)

if __name__ == "__main__":
    analyzer = BRVMAnalyzer()
    analyzer.run_and_get_results()
