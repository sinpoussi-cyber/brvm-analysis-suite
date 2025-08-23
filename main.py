# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - BRVM ANALYSIS SUITE
# ==============================================================================
import os
import logging
import sys

# Importer les modules de chaque étape
import data_collector
import fundamental_analyzer
import technical_analyzer
import report_generator  # NOUVEL IMPORT

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def main():
    """
    Fonction principale qui exécute la suite d'analyse BRVM dans l'ordre.
    1. Collecte des données quotidiennes.
    2. Analyse technique des données collectées.
    3. Analyse fondamentale des rapports de sociétés.
    4. Génération du rapport de synthèse final.
    """
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE 🚀")

    # --- Étape 1 : Collecte des données ---
    try:
        data_collector.run_data_collection()
        logging.info("✅ Étape 1/4 (Collecte de données) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec critique à l'étape 1 (Collecte de données): {e}", exc_info=True)
        sys.exit(1) # Arrête l'exécution si la collecte échoue

    # --- Étape 2 : Analyse technique ---
    try:
        technical_analyzer.run_technical_analysis()
        logging.info("✅ Étape 2/4 (Analyse technique) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 2 (Analyse technique): {e}", exc_info=True)
        sys.exit(1) # Arrête l'exécution, car le rapport final dépend des indicateurs

    # --- Étape 3 : Analyse fondamentale ---
    fundamental_results = {}
    try:
        spreadsheet_id = '1EGXyg13ml8a9zr4OaUPnJN3i-rwVO2uq330yfxJXnSM'
        google_api_key = os.environ.get('GOOGLE_API_KEY')
        
        if not google_api_key:
            logging.warning("⚠️ La variable d'environnement GOOGLE_API_KEY n'est pas définie. La partie fondamentale sera vide.")
        else:
            analyzer = fundamental_analyzer.BRVMAnalyzer(spreadsheet_id=spreadsheet_id, api_key=google_api_key)
            # Nous devons exécuter l'analyse pour obtenir les résultats en mémoire
            if analyzer.configure_gemini() and analyzer.authenticate_google_services():
                analyzer.setup_selenium()
                if analyzer.driver and analyzer.verify_and_filter_companies():
                    fundamental_results = analyzer.process_all_companies()
                if analyzer.driver:
                    analyzer.driver.quit()
                    
            logging.info("✅ Étape 3/4 (Analyse fondamentale) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 (Analyse fondamentale): {e}", exc_info=True)
        # On continue même si cette étape échoue, le rapport sera généré sans cette partie.

    # --- NOUVELLE ÉTAPE 4 : Génération du rapport de synthèse ---
    try:
        spreadsheet_id = '1EGXyg13ml8a9zr4OaUPnJN3i-rwVO2uq330yfxJXnSM'
        google_api_key = os.environ.get('GOOGLE_API_KEY')

        if not google_api_key:
            logging.warning("⚠️ GOOGLE_API_KEY non disponible. Impossible de générer le rapport de synthèse.")
        else:
            final_report_generator = report_generator.ComprehensiveReportGenerator(
                spreadsheet_id=spreadsheet_id,
                api_key=google_api_key
            )
            final_report_generator.generate_report(fundamental_results)
            logging.info("✅ Étape 4/4 (Génération du rapport de synthèse) terminée avec succès.")

    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 (Génération du rapport de synthèse): {e}", exc_info=True)

    logging.info("🏁 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🏁")


if __name__ == "__main__":
    main()
