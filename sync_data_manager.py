# ==============================================================================
# MODULE: OPTIMIZED SYNCHRONIZED DATA MANAGER (V2.0)
# Gestion des quotas Google Sheets avec Batch Processing
# ==============================================================================

import psycopg2
import gspread
from google.oauth2 import service_account
import pandas as pd
import os
import json
import logging
import time
from datetime import date
from contextlib import contextmanager
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# Limite de requêtes Google Sheets
GSHEET_REQUESTS_PER_MINUTE = 50  # Limite de sécurité (sous les 60 officiels)

class SyncDataManager:
    """Gestionnaire de synchronisation optimisé avec batch processing"""
    
    def __init__(self):
        self.db_conn = None
        self.gsheet_client = None
        self.spreadsheet = None
        self.sync_errors = []
        
        # Batch buffers pour regrouper les écritures
        self.technical_batch = defaultdict(list)  # {symbol: [data1, data2, ...]}
        self.fundamental_batch = defaultdict(list)
        
        # Rate limiting
        self.gsheet_request_times = []
        
    def _connect_db(self):
        """Établit la connexion à Supabase/PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                dbname=DB_NAME, 
                user=DB_USER, 
                password=DB_PASSWORD, 
                host=DB_HOST, 
                port=DB_PORT
            )
            logging.info("✅ Connexion Supabase établie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur connexion Supabase: {e}")
            return False
    
    def _connect_gsheet(self):
        """Établit la connexion à Google Sheets"""
        try:
            creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.gsheet_client = gspread.authorize(creds)
            self.spreadsheet = self.gsheet_client.open_by_key(SPREADSHEET_ID)
            logging.info("✅ Connexion Google Sheets établie.")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur connexion Google Sheets: {e}")
            return False
    
    def _check_rate_limit(self):
        """Vérifie et applique le rate limiting pour Google Sheets"""
        now = time.time()
        # Garder seulement les requêtes des 60 dernières secondes
        self.gsheet_request_times = [t for t in self.gsheet_request_times if now - t < 60]
        
        if len(self.gsheet_request_times) >= GSHEET_REQUESTS_PER_MINUTE:
            sleep_time = 60 - (now - self.gsheet_request_times[0]) + 1
            logging.warning(f"⏸️ Rate limit Google Sheets atteint. Pause de {sleep_time:.1f} secondes...")
            time.sleep(sleep_time)
            self.gsheet_request_times = []
        
        self.gsheet_request_times.append(time.time())
    
    @contextmanager
    def transaction_context(self):
        """Context manager pour gérer les transactions simultanées"""
        db_cursor = None
        try:
            if not self.db_conn or self.db_conn.closed:
                if not self._connect_db():
                    raise Exception("Impossible de se connecter à Supabase")
            
            if not self.gsheet_client:
                if not self._connect_gsheet():
                    raise Exception("Impossible de se connecter à Google Sheets")
            
            db_cursor = self.db_conn.cursor()
            yield db_cursor
            
            # Si tout s'est bien passé, on commit la transaction DB
            self.db_conn.commit()
            logging.info("✅ Transaction Supabase committée avec succès.")
            
        except Exception as e:
            # En cas d'erreur, on rollback la DB
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.rollback()
                logging.error(f"❌ Rollback Supabase effectué suite à une erreur: {e}")
            raise
        finally:
            if db_cursor:
                db_cursor.close()
    
    def sync_historical_data(self, company_id, symbol, trade_date, price, volume, value):
        """
        Écrit simultanément dans Supabase ET Google Sheets
        """
        with self.transaction_context() as cursor:
            # 1. Insertion dans Supabase
            try:
                cursor.execute("""
                    INSERT INTO historical_data (company_id, trade_date, price, volume, value)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, trade_date) DO UPDATE SET
                        price = EXCLUDED.price,
                        volume = EXCLUDED.volume,
                        value = EXCLUDED.value
                    RETURNING id;
                """, (company_id, trade_date, price, volume, value))
                
                result = cursor.fetchone()
                if result:
                    logging.info(f"  ✓ Supabase: {symbol} - {trade_date}")
                
            except Exception as e:
                logging.error(f"  ✗ Erreur Supabase pour {symbol}: {e}")
                raise
            
            # 2. Écriture dans Google Sheets avec rate limiting
            try:
                self._check_rate_limit()
                worksheet = self.spreadsheet.worksheet(symbol)
                
                # Vérifier si la ligne existe déjà (avec rate limit)
                self._check_rate_limit()
                existing_data = worksheet.get_all_values()
                date_str = trade_date.strftime('%d/%m/%Y')
                
                row_exists = False
                for idx, row in enumerate(existing_data[1:], start=2):
                    if len(row) > 1 and row[1] == date_str:
                        self._check_rate_limit()
                        worksheet.update(f'A{idx}:D{idx}', [[symbol, date_str, price, volume]])
                        row_exists = True
                        break
                
                if not row_exists:
                    self._check_rate_limit()
                    worksheet.append_row([symbol, date_str, price, volume], value_input_option='USER_ENTERED')
                
                logging.info(f"  ✓ Google Sheets: {symbol} - {trade_date}")
                
            except gspread.exceptions.WorksheetNotFound:
                logging.warning(f"  ⚠️ Feuille '{symbol}' non trouvée. Création...")
                try:
                    self._check_rate_limit()
                    worksheet = self.spreadsheet.add_worksheet(title=symbol, rows=1000, cols=10)
                    self._check_rate_limit()
                    worksheet.append_row(['Symbole', 'Date', 'Cours', 'Volume'], value_input_option='USER_ENTERED')
                    self._check_rate_limit()
                    worksheet.append_row([symbol, trade_date.strftime('%d/%m/%Y'), price, volume], value_input_option='USER_ENTERED')
                    logging.info(f"  ✓ Feuille '{symbol}' créée.")
                except Exception as create_error:
                    logging.error(f"  ✗ Impossible de créer la feuille '{symbol}': {create_error}")
                    raise
                    
            except Exception as e:
                logging.error(f"  ✗ Erreur Google Sheets pour {symbol}: {e}")
                raise
    
    def add_to_technical_batch(self, symbol, historical_data_id, analysis_data):
        """Ajoute des données au batch d'analyses techniques"""
        self.technical_batch[symbol].append({
            'historical_data_id': historical_data_id,
            'data': analysis_data
        })
    
    def flush_technical_batch(self):
        """
        Écrit tous les batchs d'analyses techniques en une seule fois
        C'EST ICI QU'ON OPTIMISE : 1 appel API par société au lieu de 50+
        """
        if not self.technical_batch:
            return
        
        with self.transaction_context() as cursor:
            for symbol, batch_data in self.technical_batch.items():
                logging.info(f"--- Flush batch analyse technique pour {symbol}: {len(batch_data)} lignes ---")
                
                # 1. Insertion en masse dans Supabase
                try:
                    for item in batch_data:
                        historical_data_id = item['historical_data_id']
                        data = item['data']
                        
                        cursor.execute("""
                            INSERT INTO technical_analysis (
                                historical_data_id, mm5, mm10, mm20, mm50, mm_decision,
                                bollinger_central, bollinger_inferior, bollinger_superior, bollinger_decision,
                                macd_line, signal_line, histogram, macd_decision,
                                rsi, rsi_decision,
                                stochastic_k, stochastic_d, stochastic_decision
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (historical_data_id) DO UPDATE SET
                                mm5 = EXCLUDED.mm5, mm10 = EXCLUDED.mm10, mm20 = EXCLUDED.mm20,
                                mm50 = EXCLUDED.mm50, mm_decision = EXCLUDED.mm_decision,
                                bollinger_central = EXCLUDED.bollinger_central,
                                bollinger_inferior = EXCLUDED.bollinger_inferior,
                                bollinger_superior = EXCLUDED.bollinger_superior,
                                bollinger_decision = EXCLUDED.bollinger_decision,
                                macd_line = EXCLUDED.macd_line, signal_line = EXCLUDED.signal_line,
                                histogram = EXCLUDED.histogram, macd_decision = EXCLUDED.macd_decision,
                                rsi = EXCLUDED.rsi, rsi_decision = EXCLUDED.rsi_decision,
                                stochastic_k = EXCLUDED.stochastic_k, stochastic_d = EXCLUDED.stochastic_d,
                                stochastic_decision = EXCLUDED.stochastic_decision;
                        """, (
                            historical_data_id,
                            data.get('mm5'), data.get('mm10'), data.get('mm20'), data.get('mm50'), data.get('mm_decision'),
                            data.get('bollinger_central'), data.get('bollinger_inferior'), data.get('bollinger_superior'), data.get('bollinger_decision'),
                            data.get('macd_line'), data.get('signal_line'), data.get('histogram'), data.get('macd_decision'),
                            data.get('rsi'), data.get('rsi_decision'),
                            data.get('stochastic_k'), data.get('stochastic_d'), data.get('stochastic_decision')
                        ))
                    
                    logging.info(f"  ✓ Supabase: {len(batch_data)} analyses techniques pour {symbol}")
                    
                except Exception as e:
                    logging.error(f"  ✗ Erreur Supabase analyse technique pour {symbol}: {e}")
                    raise
                
                # 2. Écriture BATCH dans Google Sheets (1 SEUL APPEL API)
                try:
                    tech_sheet_name = f"{symbol}_Technical"
                    
                    self._check_rate_limit()
                    try:
                        worksheet = self.spreadsheet.worksheet(tech_sheet_name)
                    except gspread.exceptions.WorksheetNotFound:
                        self._check_rate_limit()
                        worksheet = self.spreadsheet.add_worksheet(title=tech_sheet_name, rows=500, cols=20)
                        # En-têtes
                        self._check_rate_limit()
                        worksheet.append_row([
                            'Date Mise à Jour', 'MM5', 'MM10', 'MM20', 'MM50', 'Signal MM',
                            'Bollinger Central', 'Bollinger Inf', 'Bollinger Sup', 'Signal Bollinger',
                            'MACD', 'Signal Line', 'Histogram', 'Signal MACD',
                            'RSI', 'Signal RSI', 'Stoch K', 'Stoch D', 'Signal Stoch'
                        ])
                    
                    # Préparer TOUTES les lignes pour un seul append_rows
                    from datetime import datetime
                    rows_to_add = []
                    for item in batch_data:
                        data = item['data']
                        rows_to_add.append([
                            datetime.now().strftime('%Y-%m-%d %H:%M'),
                            data.get('mm5'), data.get('mm10'), data.get('mm20'),
                            data.get('mm50'), data.get('mm_decision'),
                            data.get('bollinger_central'), data.get('bollinger_inferior'),
                            data.get('bollinger_superior'), data.get('bollinger_decision'),
                            data.get('macd_line'), data.get('signal_line'),
                            data.get('histogram'), data.get('macd_decision'),
                            data.get('rsi'), data.get('rsi_decision'),
                            data.get('stochastic_k'), data.get('stochastic_d'),
                            data.get('stochastic_decision')
                        ])
                    
                    # UN SEUL APPEL API pour toutes les lignes
                    self._check_rate_limit()
                    worksheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
                    
                    logging.info(f"  ✓ Google Sheets: {len(batch_data)} analyses techniques pour {symbol}")
                    
                except Exception as e:
                    logging.error(f"  ✗ Erreur Google Sheets analyse technique pour {symbol}: {e}")
                    raise
        
        # Vider le batch après écriture réussie
        self.technical_batch.clear()
    
    def sync_fundamental_analysis(self, company_id, symbol, report_url, report_title, report_date, analysis_summary):
        """
        Synchronise les analyses fondamentales
        """
        with self.transaction_context() as cursor:
            # 1. Insertion dans Supabase
            try:
                cursor.execute("""
                    INSERT INTO fundamental_analysis (company_id, report_url, report_title, report_date, analysis_summary)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (report_url) DO UPDATE SET
                        analysis_summary = EXCLUDED.analysis_summary;
                """, (company_id, report_url, report_title, report_date, analysis_summary))
                
                logging.info(f"  ✓ Supabase: Analyse fondamentale pour {symbol}")
                
            except Exception as e:
                logging.error(f"  ✗ Erreur Supabase analyse fondamentale pour {symbol}: {e}")
                raise
            
            # 2. Mise à jour dans Google Sheets avec rate limiting
            try:
                fund_sheet_name = f"{symbol}_Fundamental"
                
                self._check_rate_limit()
                try:
                    worksheet = self.spreadsheet.worksheet(fund_sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    self._check_rate_limit()
                    worksheet = self.spreadsheet.add_worksheet(title=fund_sheet_name, rows=500, cols=5)
                    self._check_rate_limit()
                    worksheet.append_row(['Date Rapport', 'Titre Rapport', 'URL', 'Analyse IA'])
                
                # Ajouter les données
                self._check_rate_limit()
                worksheet.append_row([
                    report_date.strftime('%Y-%m-%d') if report_date else '',
                    report_title,
                    report_url,
                    analysis_summary[:50000]  # Limite Google Sheets
                ])
                
                logging.info(f"  ✓ Google Sheets: Analyse fondamentale pour {symbol}")
                
            except Exception as e:
                logging.error(f"  ✗ Erreur Google Sheets analyse fondamentale pour {symbol}: {e}")
                raise
    
    def close(self):
        """Ferme les connexions après avoir flushed tous les batchs"""
        # Flush les batchs restants
        if self.technical_batch:
            logging.warning("⚠️ Flush des batchs restants avant fermeture...")
            try:
                self.flush_technical_batch()
            except Exception as e:
                logging.error(f"❌ Erreur lors du flush final: {e}")
        
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()
            logging.info("Connexion Supabase fermée.")
