# ==============================================================================
# SCRIPT DE TEST - VÉRIFICATION DE LA SYNCHRONISATION
# ==============================================================================

import os
import psycopg2
import gspread
from google.oauth2 import service_account
import json
import logging
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Charger les secrets depuis l'environnement
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

def test_supabase_connection():
    """Test la connexion à Supabase"""
    logging.info("="*60)
    logging.info("TEST 1 : CONNEXION À SUPABASE")
    logging.info("="*60)
    
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("✅ Connexion Supabase réussie !")
        
        # Tester une requête
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM companies;")
            count = cur.fetchone()[0]
            logging.info(f"✅ Nombre de sociétés dans la base : {count}")
            
            cur.execute("SELECT COUNT(*) FROM historical_data;")
            count = cur.fetchone()[0]
            logging.info(f"✅ Nombre d'enregistrements historiques : {count}")
            
            cur.execute("SELECT COUNT(*) FROM technical_analysis;")
            count = cur.fetchone()[0]
            logging.info(f"✅ Nombre d'analyses techniques : {count}")
            
            cur.execute("SELECT COUNT(*) FROM fundamental_analysis;")
            count = cur.fetchone()[0]
            logging.info(f"✅ Nombre d'analyses fondamentales : {count}")
        
        conn.close()
        return True
        
    except Exception as e:
        logging.error(f"❌ Erreur de connexion Supabase : {e}")
        return False

def test_google_sheets_connection():
    """Test la connexion à Google Sheets"""
    logging.info("\n" + "="*60)
    logging.info("TEST 2 : CONNEXION À GOOGLE SHEETS")
    logging.info("="*60)
    
    try:
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        logging.info(f"✅ Connexion Google Sheets réussie !")
        logging.info(f"✅ Nom du fichier : {spreadsheet.title}")
        
        # Lister toutes les feuilles
        worksheets = spreadsheet.worksheets()
        logging.info(f"✅ Nombre de feuilles : {len(worksheets)}")
        logging.info(f"   Feuilles : {', '.join([ws.title for ws in worksheets[:10]])}...")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Erreur de connexion Google Sheets : {e}")
        return False

def test_data_consistency():
    """Vérifie la cohérence des données entre Supabase et Google Sheets"""
    logging.info("\n" + "="*60)
    logging.info("TEST 3 : VÉRIFICATION DE LA COHÉRENCE DES DONNÉES")
    logging.info("="*60)
    
    try:
        # Connexion Supabase
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
        # Connexion Google Sheets
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        # Récupérer les 3 dernières sociétés avec des données
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT c.symbol, c.name
                FROM companies c
                INNER JOIN historical_data hd ON c.id = hd.company_id
                ORDER BY c.symbol
                LIMIT 3;
            """)
            test_companies = cur.fetchall()
        
        if not test_companies:
            logging.warning("⚠️ Aucune donnée à tester dans Supabase.")
            return True
        
        for symbol, name in test_companies:
            logging.info(f"\n--- Vérification : {symbol} - {name} ---")
            
            # Compter les enregistrements dans Supabase
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM historical_data hd
                    JOIN companies c ON hd.company_id = c.id
                    WHERE c.symbol = %s;
                """, (symbol,))
                supabase_count = cur.fetchone()[0]
            
            # Compter les enregistrements dans Google Sheets
            try:
                worksheet = spreadsheet.worksheet(symbol)
                gsheet_data = worksheet.get_all_values()
                gsheet_count = len(gsheet_data) - 1  # -1 pour enlever l'en-tête
                
                logging.info(f"  Supabase : {supabase_count} enregistrements")
                logging.info(f"  Google Sheets : {gsheet_count} enregistrements")
                
                if supabase_count == gsheet_count:
                    logging.info(f"  ✅ Données cohérentes pour {symbol}")
                else:
                    logging.warning(f"  ⚠️ Différence détectée pour {symbol} : {supabase_count} vs {gsheet_count}")
                    
            except gspread.exceptions.WorksheetNotFound:
                logging.warning(f"  ⚠️ Feuille '{symbol}' non trouvée dans Google Sheets")
        
        conn.close()
        return True
        
    except Exception as e:
        logging.error(f"❌ Erreur lors de la vérification de cohérence : {e}")
        return False

def test_sync_manager():
    """Test le gestionnaire de synchronisation avec un enregistrement test"""
    logging.info("\n" + "="*60)
    logging.info("TEST 4 : TEST DU GESTIONNAIRE DE SYNCHRONISATION")
    logging.info("="*60)
    
    try:
        from sync_data_manager import SyncDataManager
        
        sync_manager = SyncDataManager()
        
        # Récupérer l'ID de la première société
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies LIMIT 1;")
            company_id, symbol = cur.fetchone()
        
        conn.close()
        
        # Créer un enregistrement test
        test_date = date.today()
        test_price = 1000.50
        test_volume = 100
        test_value = 100050.00
        
        logging.info(f"Tentative d'insertion d'un enregistrement test pour {symbol}...")
        
        sync_manager.sync_historical_data(
            company_id=company_id,
            symbol=symbol,
            trade_date=test_date,
            price=test_price,
            volume=test_volume,
            value=test_value
        )
        
        logging.info("✅ Synchronisation test réussie !")
        logging.info("   → Vérifiez manuellement Supabase et Google Sheets")
        
        sync_manager.close()
        return True
        
    except Exception as e:
        logging.error(f"❌ Erreur lors du test du gestionnaire : {e}")
        return False

def test_api_keys():
    """Vérifie que les clés API Gemini sont configurées"""
    logging.info("\n" + "="*60)
    logging.info("TEST 5 : VÉRIFICATION DES CLÉS API GEMINI")
    logging.info("="*60)
    
    api_keys_found = []
    for i in range(1, 20):
        key = os.environ.get(f'GOOGLE_API_KEY_{i}')
        if key:
            api_keys_found.append(i)
            # Masquer la clé pour des raisons de sécurité
            masked_key = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
            logging.info(f"✅ GOOGLE_API_KEY_{i} : {masked_key}")
    
    if api_keys_found:
        logging.info(f"✅ Total : {len(api_keys_found)} clé(s) API Gemini trouvée(s)")
        return True
    else:
        logging.warning("⚠️ Aucune clé API Gemini trouvée. L'analyse fondamentale sera désactivée.")
        return True  # Pas critique, juste un avertissement

def main():
    """Exécute tous les tests"""
    logging.info("🚀 DÉMARRAGE DES TESTS DE SYNCHRONISATION 🚀\n")
    
    results = {
        "Supabase Connection": test_supabase_connection(),
        "Google Sheets Connection": test_google_sheets_connection(),
        "Data Consistency": test_data_consistency(),
        "Sync Manager": test_sync_manager(),
        "API Keys": test_api_keys()
    }
    
    # Résumé
    logging.info("\n" + "="*60)
    logging.info("RÉSUMÉ DES TESTS")
    logging.info("="*60)
    
    all_passed = True
    for test_name, result in results.items():
        status = "✅ PASSÉ" if result else "❌ ÉCHOUÉ"
        logging.info(f"{test_name:30s} : {status}")
        if not result:
            all_passed = False
    
    logging.info("="*60)
    
    if all_passed:
        logging.info("🎉 TOUS LES TESTS SONT PASSÉS ! 🎉")
        logging.info("Votre système est prêt pour la synchronisation automatique.")
    else:
        logging.error("❌ CERTAINS TESTS ONT ÉCHOUÉ")
        logging.error("Veuillez corriger les erreurs avant de continuer.")
    
    logging.info("="*60)

if __name__ == "__main__":
    main()
