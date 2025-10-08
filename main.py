# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - SYNCHRONISATION AUTOMATIQUE (V3.0)
# ==============================================================================

import os
import logging
import sys
import psycopg2

# Importer les modules de chaque étape
import data_collector  # Maintenant avec synchronisation auto
import technical_analyzer
import fundamental_analyzer
import report_generator

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def main():
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE (SYNCHRONISATION AUTO) 🚀")

    # --- Étape 1 : Collecte des données (SYNC AUTO SUPABASE + GSHEET) ---
    try:
        logging.info("\n" + "="*60)
        logging.info("ÉTAPE 1 : COLLECTE & SYNCHRONISATION DES DONNÉES")
        logging.info("="*60)
        data_collector.run_data_collection()
        logging.info("✅ Données collectées et synchronisées (Supabase + Google Sheets)")
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 1 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 2 : Analyse technique (SYNC AUTO) ---
    try:
        logging.info("\n" + "="*60)
        logging.info("ÉTAPE 2 : ANALYSE TECHNIQUE & SYNCHRONISATION")
        logging.info("="*60)
        technical_analyzer.run_technical_analysis()
        logging.info("✅ Analyses techniques calculées et synchronisées")
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 2 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 3 : Analyse fondamentale (SYNC AUTO) ---
    new_fundamental_analyses = []
    try:
        logging.info("\n" + "="*60)
        logging.info("ÉTAPE 3 : ANALYSE FONDAMENTALE & SYNCHRONISATION")
        logging.info("="*60)
        
        if any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            analyzer = fundamental_analyzer.BRVMAnalyzer()
            _, new_fundamental_analyses = analyzer.run_and_get_results()
            logging.info("✅ Analyses fondamentales générées et synchronisées")
        else:
            logging.warning("⚠️ Aucune clé API Gemini trouvée, étape 3 ignorée")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 : {e}", exc_info=True)

    # --- Étape 4 : Génération du rapport de synthèse ---
    db_connection = None
    try:
        logging.info("\n" + "="*60)
        logging.info("ÉTAPE 4 : GÉNÉRATION DES RAPPORTS")
        logging.info("="*60)
        
        if any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            DB_NAME = os.environ.get('DB_NAME')
            
            db_connection = psycopg2.connect(
                dbname=DB_NAME, 
                user=os.environ.get('DB_USER'), 
                password=os.environ.get('DB_PASSWORD'), 
                host=os.environ.get('DB_HOST'), 
                port=os.environ.get('DB_PORT')
            )
            final_report_generator = report_generator.ComprehensiveReportGenerator(db_connection)
            final_report_generator.generate_all_reports(new_fundamental_analyses)
            logging.info("✅ Rapports générés avec succès")
        else:
            logging.warning("⚠️ Aucune clé API Gemini trouvée, étape 4 ignorée")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 : {e}", exc_info=True)
    finally:
        if db_connection:
            db_connection.close()

    # --- Résumé Final ---
    logging.info("\n" + "="*60)
    logging.info("🎉 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🎉")
    logging.info("="*60)
    logging.info("✅ Toutes les données sont synchronisées :")
    logging.info("   → Supabase (base de données principale)")
    logging.info("   → Google Sheets (backup & visualisation)")
    logging.info("="*60)

if __name__ == "__main__":
    main()
