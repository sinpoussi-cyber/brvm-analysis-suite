# ==============================================================================
# MODULE: FUNDAMENTAL ANALYZER
# Description: Analyse les rapports financiers avec l'IA Gemini.
# ==============================================================================

# --- Imports ---
import gspread
import requests
from bs4 import BeautifulSoup
import time
import re
from docx import Document
from docx.shared import Pt
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
from google.oauth2 import service_account
import google.generativeai as genai

# Désactiver les avertissements de sécurité
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class BRVMAnalyzer:
    def __init__(self, spreadsheet_id, api_key):
        self.spreadsheet_id = spreadsheet_id
        self.api_key = api_key
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
        self.gc = None
        self.driver = None
        self.gemini_model = None
        self.original_societes_mapping = self.societes_mapping.copy()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        
    def setup_selenium(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("--window-size=1920,1080")
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logging.info("✅ Pilote Selenium (Chrome) démarré.")
        except Exception as e:
            logging.error(f"❌ Impossible de démarrer le pilote Selenium: {e}")
            self.driver = None

    def configure_gemini(self):
        if not self.api_key:
            logging.error("❌ Clé API Google (GOOGLE_API_KEY) non trouvée. L'analyse par IA est impossible.")
            return False
        try:
            genai.configure(api_key=self.api_key)
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash-latest')
            logging.info("✅ API Gemini configurée avec succès.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur lors de la configuration de l'API Gemini: {e}")
            return False
            
    def authenticate_google_services(self):
        logging.info("Authentification Google...")
        try:
            creds_json_str = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
            if not creds_json_str:
                logging.error("❌ Secret GSPREAD_SERVICE_ACCOUNT introuvable.")
                return False
            creds_dict = json.loads(creds_json_str)
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.gc = gspread.authorize(creds)
            logging.info("✅ Authentification Google réussie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur d'authentification : {e}")
            return False

    def verify_and_filter_companies(self):
        try:
            logging.info(f"Vérification des feuilles dans G-Sheet...")
            sheet = self.gc.open_by_key(self.spreadsheet_id)
            existing_sheets = [ws.title for ws in sheet.worksheets()]
            logging.info(f"Onglets trouvés : {existing_sheets}")
            symbols_to_keep = [s for s in self.original_societes_mapping if s in existing_sheets]
            self.societes_mapping = {k: v for k, v in self.original_societes_mapping.items() if k in symbols_to_keep}
            if not self.societes_mapping:
                logging.error("❌ ERREUR FATALE : Aucune société à analyser.")
                return False
            logging.info(f"✅ {len(self.societes_mapping)} sociétés seront analysées.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur lors de la vérification du G-Sheet: {e}")
            return False

    def _normalize_text(self, text):
        if not text: return ""
        text = text.replace('-', ' ')
        text = ''.join(c for c in unicodedata.normalize('NFD', str(text).lower()) if unicodedata.category(c) != 'Mn')
        text = re.sub(r'[^a-z0-9\s\.]', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()
    
    def _find_all_reports(self):
        if not self.driver: return {}
        base_url = "https://www.brvm.org/fr/rapports-societes-cotees"
        all_reports = defaultdict(list)
        company_links = []
        try:
            for page_num in range(5): 
                page_url = f"{base_url}?page={page_num}"
                logging.info(f"Navigation vers la page de liste : {page_url}")
                self.driver.get(page_url)
                try:
                    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.views-table")))
                except TimeoutException:
                    logging.info(f"La page {page_num} ne semble pas contenir de tableau. Fin de la pagination.")
                    break
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                table_rows = soup.select("table.views-table tbody tr")
                if not table_rows:
                    logging.info(f"Aucune société trouvée sur la page {page_num}. Fin de la pagination.")
                    break
                for row in table_rows:
                    link_tag = row.find('a', href=True)
                    if link_tag:
                        company_name_normalized = self._normalize_text(link_tag.text)
                        company_url = f"https://www.brvm.org{link_tag['href']}"
                        symbol = self._get_symbol_from_name(company_name_normalized)
                        if symbol and symbol in self.societes_mapping:
                            if not any(c['url'] == company_url for c in company_links):
                                company_links.append({'symbol': symbol, 'url': company_url})
                time.sleep(1)
            logging.info(f"Collecte des liens terminée. {len(company_links)} pages de sociétés pertinentes à visiter.")
            for company in company_links:
                symbol = company['symbol']
                logging.info(f"--- Collecte des rapports pour {symbol} ---")
                try:
                    self.driver.get(company['url'])
                    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.views-table")))
                    page_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    report_items = page_soup.select("table.views-table tbody tr")
                    if not report_items:
                        logging.warning(f"  -> Aucun rapport listé sur la page de {symbol}.")
                        continue
                    for item in report_items:
                        pdf_link_tag = item.find('a', href=lambda href: href and '.pdf' in href.lower())
                        if pdf_link_tag:
                            full_url = pdf_link_tag['href'] if pdf_link_tag['href'].startswith('http') else f"https://www.brvm.org{pdf_link_tag['href']}"
                            if not any(r['url'] == full_url for r in all_reports[symbol]):
                                report_data = {
                                    'titre': " ".join(item.get_text().split()),
                                    'url': full_url,
                                    'date': self._extract_date_from_text(item.get_text())
                                }
                                all_reports[symbol].append(report_data)
                                logging.info(f"  -> Trouvé : {report_data['titre'][:70]}...")
                    time.sleep(1)
                except TimeoutException:
                    logging.error(f"  -> Timeout sur la page de {symbol}. Passage au suivant.")
                except Exception as e:
                    logging.error(f"  -> Erreur sur la page de {symbol}: {e}. Passage au suivant.")
        except Exception as e:
            logging.error(f"Erreur critique lors du scraping : {e}", exc_info=True)
            return {}
        return all_reports

    def _get_symbol_from_name(self, company_name_normalized):
        for symbol, info in self.original_societes_mapping.items():
            for alt in info['alternatives']:
                if alt in company_name_normalized:
                    return symbol
        return None

    def _extract_date_from_text(self, text):
        if not text: return datetime(1900, 1, 1)
        year_match = re.search(r'\b(20\d{2})\b', text)
        if not year_match: return datetime(1900, 1, 1)
        year = int(year_match.group(1))
        text_lower = text.lower()
        trim_match = re.search(r't(\d)|(\d)\s*er\s*trimestre', text_lower)
        if trim_match:
            trimester = int(trim_match.group(1) or trim_match.group(2))
            return datetime(year, trimester * 3, 1)
        sem_match = re.search(r's(\d)|(\d)\s*er\s*semestre', text_lower)
        if sem_match:
            semester = int(sem_match.group(1) or sem_match.group(2))
            return datetime(year, 6 if semester == 1 else 12, 1)
        if 'annuel' in text_lower or '31/12' in text or '31 dec' in text_lower: return datetime(year, 12, 31)
        return datetime(year, 6, 15)

    def _analyze_pdf_with_gemini(self, pdf_url):
        if not self.gemini_model:
            return "Analyse IA non disponible (API non configurée)."
        
        logging.info(f"    -> Téléchargement du PDF pour l'envoyer à Gemini...")
        uploaded_file = None
        temp_pdf_path = "temp_report.pdf"
        try:
            response = self.session.get(pdf_url, timeout=45, verify=False)
            response.raise_for_status()
            pdf_content = response.content
            if len(pdf_content) < 1024:
                return "Fichier PDF invalide ou vide."
            with open(temp_pdf_path, 'wb') as f:
                f.write(pdf_content)
            logging.info(f"    -> Envoi du fichier PDF ({os.path.getsize(temp_pdf_path)} octets) à l'API Gemini...")
            uploaded_file = genai.upload_file(
                path=temp_pdf_path,
                display_name="Rapport Financier BRVM"
            )
            prompt = """
            Tu es un analyste financier expert spécialisé dans les entreprises de la zone UEMOA cotées à la BRVM.
            Analyse le document PDF ci-joint, qui est un rapport financier, et fournis une synthèse concise en français, structurée en points clés.

            Concentre-toi impérativement sur les aspects suivants :
            - **Évolution du Chiffre d'Affaires (CA)** : Indique la variation en pourcentage et en valeur si possible. Mentionne les raisons de cette évolution.
            - **Évolution du Résultat Net (RN)** : Indique la variation et les facteurs qui l'ont influencée.
            - **Politique de Dividende** : Cherche toute mention de dividende proposé, payé ou des perspectives de distribution.
            - **Performance des Activités Ordinaires/d'Exploitation** : Commente l'évolution de la rentabilité opérationnelle.
            - **Perspectives et Points de Vigilance** : Relève tout point crucial pour un investisseur (endettement, investissements majeurs, perspectives, etc.).

            Si une information n'est pas trouvée, mentionne-le clairement (ex: "Politique de dividende non mentionnée"). Sois factuel et base tes conclusions uniquement sur le document.
            """
            
            logging.info("    -> Fichier envoyé. Génération de l'analyse...")
            response = self.gemini_model.generate_content([prompt, uploaded_file])
            
            if response.parts:
                return response.text
            elif response.prompt_feedback:
                block_reason = response.prompt_feedback.block_reason.name
                error_message = f"Analyse bloquée par l'IA. Raison : {block_reason}."
                return error_message
            else:
                 return "Erreur inconnue : L'API Gemini n'a retourné ni contenu ni feedback."

        except Exception as e:
            error_details = f"Erreur technique lors de l'analyse par l'IA : {str(e)}"
            return error_details
        finally:
            if uploaded_file:
                try:
                    logging.info(f"    -> Suppression du fichier temporaire de l'API Gemini.")
                    genai.delete_file(uploaded_file.name)
                except Exception as e:
                    logging.warning(f"    -> N'a pas pu supprimer le fichier temporaire de l'API : {e}")

            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)
                logging.info(f"    -> Suppression du fichier PDF local ({temp_pdf_path}).")

    def process_all_companies(self):
        all_reports = self._find_all_reports()
        results = {}
        if not all_reports:
            logging.error("❌ ÉCHEC FINAL : Aucun rapport n'a pu être collecté sur le site de la BRVM.")
            return {}
        logging.info(f"\n✅ COLLECTE TERMINÉE : {sum(len(r) for r in all_reports.values())} rapports trouvés au total.")
        
        date_2024_start = datetime(2024, 1, 1)
        date_2025_start = datetime(2025, 1, 1)
        keywords_financiers = ['états financiers', 'etats financiers', 'certifié', 'commissaires aux comptes', 'rapport annuel']

        for symbol, info in self.societes_mapping.items():
            logging.info(f"\n📊 Traitement des données pour {symbol} - {info['nom_rapport']}")
            
            company_reports = all_reports.get(symbol, [])
            analysis_data = {'nom': info['nom_rapport'], 'rapports_analyses': []}
            reports_to_analyze = []
            
            for report in company_reports:
                report_date = report['date']
                title_lower = report['titre'].lower()
                if date_2024_start <= report_date < date_2025_start:
                    if any(keyword in title_lower for keyword in keywords_financiers):
                        reports_to_analyze.append(report)
                elif report_date >= date_2025_start:
                    reports_to_analyze.append(report)
            
            reports_to_analyze.sort(key=lambda x: x['date'], reverse=True)
            
            if not reports_to_analyze:
                analysis_data['statut'] = 'Aucun rapport pertinent trouvé selon les critères de filtrage (date/titre).'
                results[symbol] = analysis_data
                continue
            
            logging.info(f"  -> {len(reports_to_analyze)} rapport(s) pertinent(s) trouvé(s) après filtrage.")

            for i, report in enumerate(reports_to_analyze):
                logging.info(f"  -> Analyse IA {i+1}/{len(reports_to_analyze)}: {report['titre'][:60]}...")
                gemini_analysis = self._analyze_pdf_with_gemini(report['url'])
                analysis_data['rapports_analyses'].append({
                    'titre': report['titre'], 
                    'url': report['url'], 
                    'date': report['date'].strftime('%Y-%m-%d'),
                    'analyse_ia': gemini_analysis
                })
                time.sleep(3)
            
            results[symbol] = analysis_data
        
        logging.info("\n✅ Traitement de toutes les sociétés terminé.")
        return results

    def create_word_report(self, results, output_path):
        logging.info(f"Création du rapport Word : {output_path}")
        try:
            doc = Document()
            doc.add_heading('Analyse Financière des Sociétés Cotées par IA (Gemini)', 0)
            doc.add_paragraph(f"Rapport généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}")
            doc.add_paragraph("Ce rapport analyse les états financiers certifiés pour l'année 2024 et tous les rapports publiés à partir de 2025.")

            for symbol, data in results.items():
                doc.add_heading(f"{symbol} - {data['nom']}", level=2)
                
                if not data.get('rapports_analyses'):
                    status_message = data.get('statut', 'Aucun rapport pertinent n\'a été trouvé.')
                    doc.add_paragraph(f"❌ {status_message}")
                    continue
                
                table = doc.add_table(rows=1, cols=2, style='Table Grid')
                table.autofit = False
                table.columns[0].width = Pt(150)
                table.columns[1].width = Pt(350)
                headers = ['Titre du Rapport / Date de Publication', "Synthèse de l'Analyse par l'IA"]
                header_cells = table.rows[0].cells
                header_cells[0].text = headers[0]
                header_cells[1].text = headers[1]

                for rapport in data['rapports_analyses']:
                    row_cells = table.add_row().cells
                    cell_0_p = row_cells[0].paragraphs[0]
                    cell_0_p.add_run(rapport['titre']).bold = True
                    cell_0_p.add_run(f"\n(Date extraite : {rapport['date']})").italic = True
                    row_cells[1].text = rapport.get('analyse_ia', 'Analyse non disponible.')

                doc.add_paragraph()

            doc.save(output_path)
            logging.info(f"🎉 RAPPORT FINALISÉ. Fichier sauvegardé : {output_path}")
        except Exception as e:
            logging.error(f"❌ Impossible d'enregistrer le rapport Word : {e}", exc_info=True)

    def run_fundamental_analysis(self):
        logging.info("="*60)
        logging.info("ÉTAPE 2 : DÉMARRAGE DE L'ANALYSE FONDAMENTALE (IA)")
        logging.info("="*60)
        
        try:
            if not self.configure_gemini(): return
            self.setup_selenium()
            if not self.driver or not self.authenticate_google_services(): return
            if not self.verify_and_filter_companies(): return
            analysis_results = self.process_all_companies()
            if analysis_results:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M')
                output_filename = f"Analyse_Financiere_BRVM_{timestamp}.docx"
                self.create_word_report(analysis_results, output_filename)
            else:
                logging.warning("❌ Aucun résultat d'analyse à inclure dans le rapport.")
        except Exception as e:
            logging.critical(f"❌ Une erreur critique a interrompu l'analyse: {e}", exc_info=True)
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Navigateur Selenium fermé.")
            logging.info("Processus d'analyse fondamentale terminé.")
