# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - BRVM ANALYSIS SUITE (V1.6 - Final)
# ==============================================================================
import os
import logging
import sys

# Importer les modules de chaque étape
import data_collector
import fundamental_analyzer
import technical_analyzer
import report_generator

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def main():
    """
    Fonction principale qui exécute la suite d'analyse BRVM dans l'ordre.
    1. Collecte des données quotidiennes.
    2. Analyse technique des données collectées.
    3. Analyse fondamentale des rapports de sociétés avec mémoire et rotation de clés.
    4. Génération des rapports de synthèse et sauvegarde sur Drive.
    """
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE 🚀")
    
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    drive_folder_id = os.environ.get('DRIVE_FOLDER_ID')

    if not spreadsheet_id or not drive_folder_id:
        logging.error("❌ Les secrets SPREADSHEET_ID ou DRIVE_FOLDER_ID ne sont pas définis. Arrêt du script.")
        sys.exit(1)
        
    # On assigne l'ID globalement aux modules qui l'utilisent en dur
    data_collector.SPREADSHEET_ID = spreadsheet_id
    technical_analyzer.SPREADSHEET_ID = spreadsheet_id

    # --- Étape 1 : Collecte des données ---
    try:
        data_collector.run_data_collection()
        logging.info("✅ Étape 1/4 (Collecte de données) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec critique à l'étape 1 (Collecte de données): {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 2 : Analyse technique ---
    try:
        logging.info("="*60)
        logging.info("ÉTAPE 2 : DÉMARRAGE DE L'ANALYSE TECHNIQUE")
        logging.info("="*60)
        technical_analyzer.run_technical_analysis()
        logging.info("✅ Étape 2/4 (Analyse technique) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 2 (Analyse technique): {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 3 : Analyse fondamentale ---
    fundamental_results = {}
    new_fundamental_analyses = []
    try:
        if not any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            logging.warning("⚠️ Aucune variable d'environnement GOOGLE_API_KEY_n n'est définie. L'étape fondamentale sera sautée.")
        else:
            analyzer = fundamental_analyzer.BRVMAnalyzer(spreadsheet_id=spreadsheet_id)
            fundamental_results, new_fundamental_analyses = analyzer.run_and_get_results()
            logging.info("✅ Étape 3/4 (Analyse fondamentale) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 (Analyse fondamentale): {e}", exc_info=True)

    # --- Étape 4 : Génération du rapport de synthèse ---
    try:
        if not any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            logging.warning("⚠️ Aucune clé API n'est disponible. Impossible de générer les rapports.")
        else:
            final_report_generator = report_generator.ComprehensiveReportGenerator(
                spreadsheet_id=spreadsheet_id,
                drive_folder_id=drive_folder_id
            )
            final_report_generator.generate_report(fundamental_results, new_fundamental_analyses)
            logging.info("✅ Étape 4/4 (Génération des rapports) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 (Génération des rapports): {e}", exc_info=True)

    logging.info("🏁 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🏁")


if __name__ == "__main__":
    main()
