# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - ARCHITECTURE POSTGRESQL (V2.2 - AVEC EXPORT GSHEET)
# ==============================================================================

import os
import logging
import sys
import psycopg2

# Importer les modules de chaque étape
import data_collector
import technical_analyzer
import fundamental_analyzer
import report_generator
import export_to_gsheet # <-- NOUVEL IMPORT

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def main():
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE (ARCHITECTURE DB) 🚀")

    # --- Étape 1 : Collecte des données ---
    try:
        data_collector.run_data_collection()
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 1 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 2 : Analyse technique ---
    try:
        technical_analyzer.run_technical_analysis()
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 2 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 3 : Analyse fondamentale ---
    new_fundamental_analyses = []
    try:
        if any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            analyzer = fundamental_analyzer.BRVMAnalyzer()
            _, new_fundamental_analyses = analyzer.run_and_get_results()
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 : {e}", exc_info=True)

    # --- Étape 4 : Génération du rapport de synthèse ---
    db_connection = None
    try:
        if any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            DB_NAME = os.environ.get('DB_NAME')
            # ... (récupération des autres secrets DB)
            
            db_connection = psycopg2.connect(dbname=DB_NAME, user=os.environ.get('DB_USER'), password=os.environ.get('DB_PASSWORD'), host=os.environ.get('DB_HOST'), port=os.environ.get('DB_PORT'))
            final_report_generator = report_generator.ComprehensiveReportGenerator(db_connection)
            final_report_generator.generate_all_reports(new_fundamental_analyses)
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 : {e}", exc_info=True)
    finally:
        if db_connection:
            db_connection.close()

    # --- ÉTAPE SUPPLÉMENTAIRE : Export vers Google Sheets ---
    try:
        if os.environ.get('SPREADSHEET_ID') and os.environ.get('GSPREAD_SERVICE_ACCOUNT'):
            export_to_gsheet.export_today_data()
        else:
            logging.warning("Secrets Google Sheets non trouvés, l'export est ignoré.")
    except Exception as e:
        logging.error(f"❌ Échec de l'étape d'export vers Google Sheets : {e}", exc_info=True)

    logging.info("🏁 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🏁")

if __name__ == "__main__":
    main()
