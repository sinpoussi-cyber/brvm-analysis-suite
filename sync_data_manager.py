# ==============================================================================
# MODULE: SYNCHRONIZED DATA MANAGER (V1.0)
# Gère l'écriture simultanée vers Google Sheets ET Supabase
# ==============================================================================

import psycopg2
import gspread
from google.oauth2 import service_account
import pandas as pd
import os
import json
import logging
from datetime import date
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

class SyncDataManager:
    """Gestionnaire de synchronisation entre Google Sheets et Supabase"""
    
    def __init__(self):
        self.db_conn = None
        self.gsheet_client = None
        self.spreadsheet = None
        self.sync_errors = []
        
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
        Si l'une des écritures échoue, aucune des deux n'est persistée (rollback)
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
                raise  # Déclenche le rollback
            
            # 2. Écriture dans Google Sheets (seulement si Supabase a réussi)
            try:
                worksheet = self.spreadsheet.worksheet(symbol)
                
                # Vérifier si la ligne existe déjà
                existing_data = worksheet.get_all_values()
                date_str = trade_date.strftime('%d/%m/%Y')
                
                row_exists = False
                for idx, row in enumerate(existing_data[1:], start=2):  # Skip header
                    if len(row) > 1 and row[1] == date_str:
                        # Mettre à jour la ligne existante
                        worksheet.update(f'A{idx}:D{idx}', [[symbol, date_str, price, volume]])
                        row_exists = True
                        break
                
                if not row_exists:
                    # Ajouter une nouvelle ligne
                    worksheet.append_row([symbol, date_str, price, volume], value_input_option='USER_ENTERED')
                
                logging.info(f"  ✓ Google Sheets: {symbol} - {trade_date}")
                
            except gspread.exceptions.WorksheetNotFound:
                logging.warning(f"  ⚠️ Feuille '{symbol}' non trouvée dans Google Sheets. Création...")
                try:
                    worksheet = self.spreadsheet.add_worksheet(title=symbol, rows=1000, cols=10)
                    worksheet.append_row(['Symbole', 'Date', 'Cours', 'Volume'], value_input_option='USER_ENTERED')
                    worksheet.append_row([symbol, trade_date.strftime('%d/%m/%Y'), price, volume], value_input_option='USER_ENTERED')
                    logging.info(f"  ✓ Feuille '{symbol}' créée et données ajoutées.")
                except Exception as create_error:
                    logging.error(f"  ✗ Impossible de créer la feuille '{symbol}': {create_error}")
                    raise  # Déclenche le rollback
                    
            except Exception as e:
                logging.error(f"  ✗ Erreur Google Sheets pour {symbol}: {e}")
                raise  # Déclenche le rollback
    
    def sync_technical_analysis(self, historical_data_id, symbol, analysis_data):
        """
        Synchronise les analyses techniques
        """
        with self.transaction_context() as cursor:
            # 1. Insertion dans Supabase
            try:
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
                        mm5 = EXCLUDED.mm5, mm10 = EXCLUDED.mm10, mm20 = EXCLUDED.mm20, mm50 = EXCLUDED.mm50, mm_decision = EXCLUDED.mm_decision,
                        bollinger_central = EXCLUDED.bollinger_central, bollinger_inferior = EXCLUDED.bollinger_inferior, 
                        bollinger_superior = EXCLUDED.bollinger_superior, bollinger_decision = EXCLUDED.bollinger_decision,
                        macd_line = EXCLUDED.macd_line, signal_line = EXCLUDED.signal_line, histogram = EXCLUDED.histogram, macd_decision = EXCLUDED.macd_decision,
                        rsi = EXCLUDED.rsi, rsi_decision = EXCLUDED.rsi_decision,
                        stochastic_k = EXCLUDED.stochastic_k, stochastic_d = EXCLUDED.stochastic_d, stochastic_decision = EXCLUDED.stochastic_decision;
                """, (
                    historical_data_id,
                    analysis_data.get('mm5'), analysis_data.get('mm10'), analysis_data.get('mm20'), analysis_data.get('mm50'), analysis_data.get('mm_decision'),
                    analysis_data.get('bollinger_central'), analysis_data.get('bollinger_inferior'), analysis_data.get('bollinger_superior'), analysis_data.get('bollinger_decision'),
                    analysis_data.get('macd_line'), analysis_data.get('signal_line'), analysis_data.get('histogram'), analysis_data.get('macd_decision'),
                    analysis_data.get('rsi'), analysis_data.get('rsi_decision'),
                    analysis_data.get('stochastic_k'), analysis_data.get('stochastic_d'), analysis_data.get('stochastic_decision')
                ))
                
                logging.info(f"  ✓ Supabase: Analyse technique pour {symbol}")
                
            except Exception as e:
                logging.error(f"  ✗ Erreur Supabase analyse technique pour {symbol}: {e}")
                raise
            
            # 2. Mise à jour dans Google Sheets (optionnel - peut créer une feuille dédiée aux analyses)
            try:
                tech_sheet_name = f"{symbol}_Technical"
                try:
                    worksheet = self.spreadsheet.worksheet(tech_sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = self.spreadsheet.add_worksheet(title=tech_sheet_name, rows=500, cols=20)
                    # En-têtes
                    worksheet.append_row([
                        'Date Mise à Jour', 'MM5', 'MM10', 'MM20', 'MM50', 'Signal MM',
                        'Bollinger Central', 'Bollinger Inf', 'Bollinger Sup', 'Signal Bollinger',
                        'MACD', 'Signal Line', 'Histogram', 'Signal MACD',
                        'RSI', 'Signal RSI', 'Stoch K', 'Stoch D', 'Signal Stoch'
                    ])
                
                # Ajouter les données
                from datetime import datetime
                worksheet.append_row([
                    datetime.now().strftime('%Y-%m-%d %H:%M'),
                    analysis_data.get('mm5'), analysis_data.get('mm10'), analysis_data.get('mm20'), 
                    analysis_data.get('mm50'), analysis_data.get('mm_decision'),
                    analysis_data.get('bollinger_central'), analysis_data.get('bollinger_inferior'), 
                    analysis_data.get('bollinger_superior'), analysis_data.get('bollinger_decision'),
                    analysis_data.get('macd_line'), analysis_data.get('signal_line'), 
                    analysis_data.get('histogram'), analysis_data.get('macd_decision'),
                    analysis_data.get('rsi'), analysis_data.get('rsi_decision'),
                    analysis_data.get('stochastic_k'), analysis_data.get('stochastic_d'), 
                    analysis_data.get('stochastic_decision')
                ])
                
                logging.info(f"  ✓ Google Sheets: Analyse technique pour {symbol}")
                
            except Exception as e:
                logging.error(f"  ✗ Erreur Google Sheets analyse technique pour {symbol}: {e}")
                raise
    
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
            
            # 2. Mise à jour dans Google Sheets
            try:
                fund_sheet_name = f"{symbol}_Fundamental"
                try:
                    worksheet = self.spreadsheet.worksheet(fund_sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = self.spreadsheet.add_worksheet(title=fund_sheet_name, rows=500, cols=5)
                    worksheet.append_row(['Date Rapport', 'Titre Rapport', 'URL', 'Analyse IA'])
                
                # Ajouter les données
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
        """Ferme les connexions"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()
            logging.info("Connexion Supabase fermée.")


# ==============================================================================
# FONCTION PRINCIPALE D'EXPORT
# ==============================================================================

def run_synchronized_export():
    """
    Fonction principale qui orchestre la synchronisation
    """
    logging.info("="*60)
    logging.info("DÉMARRAGE DE LA SYNCHRONISATION SUPABASE ↔ GOOGLE SHEETS")
    logging.info("="*60)
    
    sync_manager = SyncDataManager()
    
    try:
        # Connexion initiale
        if not sync_manager._connect_db():
            raise Exception("Impossible de se connecter à Supabase")
        if not sync_manager._connect_gsheet():
            raise Exception("Impossible de se connecter à Google Sheets")
        
        # Récupérer toutes les données à synchroniser
        with sync_manager.db_conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.symbol, hd.trade_date, hd.price, hd.volume, hd.value
                FROM historical_data hd
                JOIN companies c ON hd.company_id = c.id
                WHERE hd.trade_date = CURRENT_DATE
                ORDER BY c.symbol;
            """)
            today_data = cur.fetchall()
        
        if not today_data:
            logging.warning("Aucune donnée pour aujourd'hui à synchroniser.")
            return
        
        logging.info(f"Synchronisation de {len(today_data)} enregistrements...")
        
        success_count = 0
        error_count = 0
        
        for company_id, symbol, trade_date, price, volume, value in today_data:
            try:
                sync_manager.sync_historical_data(company_id, symbol, trade_date, price, volume, value)
                success_count += 1
            except Exception as e:
                error_count += 1
                logging.error(f"❌ Échec synchronisation pour {symbol}: {e}")
        
        logging.info("="*60)
        logging.info(f"✅ Synchronisation terminée: {success_count} réussies, {error_count} échecs")
        logging.info("="*60)
        
    except Exception as e:
        logging.critical(f"❌ Erreur critique lors de la synchronisation: {e}", exc_info=True)
    finally:
        sync_manager.close()


if __name__ == "__main__":
    run_synchronized_export()
