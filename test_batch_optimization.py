# ==============================================================================
# SCRIPT DE TEST - VÉRIFICATION DE L'OPTIMISATION BATCH
# ==============================================================================

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def test_batch_processing():
    """
    Test le batch processing avec 3 sociétés seulement
    pour vérifier que l'optimisation fonctionne
    """
    logging.info("="*60)
    logging.info("TEST DE L'OPTIMISATION BATCH PROCESSING")
    logging.info("="*60)
    
    try:
        from sync_data_manager import SyncDataManager
        import psycopg2
        
        # Vérifier les variables d'environnement
        required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 
                        'SPREADSHEET_ID', 'GSPREAD_SERVICE_ACCOUNT']
        
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            logging.error(f"❌ Variables manquantes : {', '.join(missing_vars)}")
            return False
        
        # Connexion à la base de données
        conn = psycopg2.connect(
            dbname=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            host=os.environ.get('DB_HOST'),
            port=os.environ.get('DB_PORT')
        )
        logging.info("✅ Connexion DB réussie")
        
        # Récupérer 3 sociétés de test
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies ORDER BY symbol LIMIT 3;")
            test_companies = cur.fetchall()
        
        if not test_companies:
            logging.error("❌ Aucune société trouvée dans la base")
            return False
        
        logging.info(f"📊 Test avec {len(test_companies)} sociétés : {[s[1] for s in test_companies]}")
        
        # Initialiser le sync manager
        sync_manager = SyncDataManager()
        start_time = time.time()
        total_api_calls = 0
        
        for company_id, symbol in test_companies:
            logging.info(f"\n--- Test {symbol} ---")
            
            # Récupérer quelques données
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM historical_data 
                    WHERE company_id = %s 
                    ORDER BY trade_date DESC 
                    LIMIT 10;
                """, (company_id,))
                historical_data_ids = [row[0] for row in cur.fetchall()]
            
            if not historical_data_ids:
                logging.warning(f"  ⚠️ Pas de données pour {symbol}")
                continue
            
            logging.info(f"  → Ajout de {len(historical_data_ids)} lignes au batch...")
            
            # Simuler l'ajout au batch
            for hd_id in historical_data_ids:
                fake_data = {
                    'mm5': 1000.0, 'mm10': 1010.0, 'mm20': 1020.0, 'mm50': 1050.0,
                    'mm_decision': 'Test', 'bollinger_central': 1000.0,
                    'bollinger_inferior': 950.0, 'bollinger_superior': 1050.0,
                    'bollinger_decision': 'Test', 'macd_line': 10.0,
                    'signal_line': 8.0, 'histogram': 2.0, 'macd_decision': 'Test',
                    'rsi': 50.0, 'rsi_decision': 'Test', 'stochastic_k': 50.0,
                    'stochastic_d': 48.0, 'stochastic_decision': 'Test'
                }
                sync_manager.add_to_technical_batch(symbol, hd_id, fake_data)
            
            # Flush le batch
            logging.info(f"  → Flush du batch pour {symbol}...")
            batch_start = time.time()
            
            try:
                sync_manager.flush_technical_batch()
                batch_time = time.time() - batch_start
                
                # Compter les appels API (rate limit tracker)
                api_calls = len(sync_manager.gsheet_request_times)
                total_api_calls += api_calls
                
                logging.info(f"  ✅ Batch flushed en {batch_time:.2f}s")
                logging.info(f"  📊 Appels API pour cette société : {api_calls}")
                
            except Exception as e:
                logging.error(f"  ❌ Erreur lors du flush : {e}")
                return False
            
            # Petite pause entre sociétés
            time.sleep(1)
        
        total_time = time.time() - start_time
        
        # Résumé
        logging.info("\n" + "="*60)
        logging.info("RÉSUMÉ DU TEST")
        logging.info("="*60)
        logging.info(f"Sociétés testées : {len(test_companies)}")
        logging.info(f"Temps total : {total_time:.2f} secondes")
        logging.info(f"Appels API Google Sheets : {total_api_calls}")
        logging.info(f"Moyenne par société : {total_api_calls / len(test_companies):.1f} appels")
        
        # Vérification
        if total_api_calls <= len(test_companies) * 5:  # Max 5 appels par société
            logging.info("✅ OPTIMISATION RÉUSSIE : Nombre d'appels API acceptable")
            success = True
        else:
            logging.warning(f"⚠️ OPTIMISATION PARTIELLE : {total_api_calls} appels API (attendu < {len(test_companies) * 5})")
            success = False
        
        # Vérifier dans Google Sheets
        logging.info("\n" + "="*60)
        logging.info("VÉRIFICATION DANS GOOGLE SHEETS")
        logging.info("="*60)
        
        for company_id, symbol in test_companies:
            try:
                worksheet = sync_manager.spreadsheet.worksheet(f"{symbol}_Technical")
                row_count = len(worksheet.get_all_values()) - 1  # -1 pour l'en-tête
                logging.info(f"  {symbol}_Technical : {row_count} lignes")
            except Exception as e:
                logging.warning(f"  {symbol}_Technical : Feuille non trouvée ou erreur")
        
        conn.close()
        sync_manager.close()
        
        logging.info("="*60)
        return success
        
    except Exception as e:
        logging.error(f"❌ Erreur critique : {e}", exc_info=True)
        return False

def main():
    logging.info("🚀 DÉMARRAGE DU TEST D'OPTIMISATION BATCH\n")
    
    success = test_batch_processing()
    
    if success:
        logging.info("\n🎉 TEST RÉUSSI ! L'optimisation fonctionne correctement.")
        logging.info("Vous pouvez maintenant exécuter l'analyse complète sur toutes les sociétés.")
    else:
        logging.error("\n❌ TEST ÉCHOUÉ. Vérifiez les logs ci-dessus pour identifier le problème.")
    
    logging.info("\n" + "="*60)

if __name__ == "__main__":
    main()
