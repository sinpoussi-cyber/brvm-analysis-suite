# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - BRVM ANALYSIS SUITE (V1.5 - Corrigé)
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
    4. Génération du rapport de synthèse final.
    """
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE 🚀")
    
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    if not spreadsheet_id:
        logging.error("❌ Le secret SPREADSHEET_ID n'est pas défini. Arrêt du script.")
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
    try:
        if not any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            logging.warning("⚠️ Aucune variable d'environnement GOOGLE_API_KEY_n n'est définie. L'étape fondamentale sera sautée.")
        else:
            analyzer = fundamental_analyzer.BRVMAnalyzer(spreadsheet_id=spreadsheet_id)
            fundamental_results = analyzer.run_and_get_results()
            logging.info("✅ Étape 3/4 (Analyse fondamentale) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 (Analyse fondamentale): {e}", exc_info=True)

    # --- Étape 4 : Génération du rapport de synthèse ---
    try:
        if not any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20)):
            logging.warning("⚠️ Aucune clé API n'est disponible. Impossible de générer le rapport de synthèse.")
        else:
            final_report_generator = report_generator.ComprehensiveReportGenerator(
                spreadsheet_id=spreadsheet_id
            )
            final_report_generator.generate_report(fundamental_results)
            logging.info("✅ Étape 4/4 (Génération du rapport de synthèse) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 (Génération du rapport de synthèse): {e}", exc_info=True)

    logging.info("🏁 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🏁")


if __name__ == "__main__":
    main()
