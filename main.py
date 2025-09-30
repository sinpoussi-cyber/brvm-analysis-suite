# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - ARCHITECTURE POSTGRESQL (V2.0)
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

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def main():
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE (ARCHITECTURE DB) 🚀")

    # --- Étape 1 : Collecte des données ---
    try:
        data_collector.run_data_collection()
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 1 (Collecte de données): {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 2 : Analyse technique ---
    try:
        technical_analyzer.run_technical_analysis()
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 2 (Analyse technique): {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 3 : Analyse fondamentale ---
    fundamental_results = {}
    new_fundamental_analyses = []
    try:
        if not any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            logging.warning("⚠️ Aucune clé API Gemini. L'étape d'analyse fondamentale et de reporting sera sautée.")
        else:
            analyzer = fundamental_analyzer.BRVMAnalyzer()
            fundamental_results, new_fundamental_analyses = analyzer.run_and_get_results()
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 (Analyse fondamentale): {e}", exc_info=True)

    # --- Étape 4 : Génération du rapport de synthèse ---
    db_connection = None
    try:
        if not any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            logging.warning("⚠️ Aucune clé API Gemini. Impossible de générer les rapports.")
        else:
            DB_NAME = os.environ.get('DB_NAME')
            DB_USER = os.environ.get('DB_USER')
            DB_PASSWORD = os.environ.get('DB_PASSWORD')
            DB_HOST = os.environ.get('DB_HOST')
            DB_PORT = os.environ.get('DB_PORT')
            
            db_connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            final_report_generator = report_generator.ComprehensiveReportGenerator(db_connection)
            final_report_generator.generate_all_reports(new_fundamental_analyses)

    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 (Génération des rapports): {e}", exc_info=True)
    finally:
        if db_connection:
            db_connection.close()

    logging.info("🏁 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🏁")

if __name__ == "__main__":
    main()
