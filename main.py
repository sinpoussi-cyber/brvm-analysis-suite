# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - BRVM ANALYSIS SUITE
# ==============================================================================
import os
import logging

# Importer les modules de chaque étape
import data_collector
import fundamental_analyzer
import technical_analyzer

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def main():
    """
    Fonction principale qui exécute la suite d'analyse BRVM dans l'ordre.
    1. Collecte des données quotidiennes.
    2. Analyse fondamentale des rapports de sociétés.
    3. Analyse technique des données collectées.
    """
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE 🚀")

    # --- Étape 1 : Collecte des données ---
    try:
        data_collector.run_data_collection()
        logging.info("✅ Étape 1/3 (Collecte de données) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec critique à l'étape 1 (Collecte de données): {e}", exc_info=True)
        # On arrête le processus si la collecte échoue, car les étapes suivantes en dépendent.
        return 

    # --- Étape 2 : Analyse fondamentale ---
    try:
        # Récupérer les identifiants depuis les secrets GitHub
        spreadsheet_id = '1EGXyg13ml8a9zr4OaUPnJN3i-rwVO2uq330yfxJXnSM'
        google_api_key = os.environ.get('GOOGLE_API_KEY')
        
        if not google_api_key:
            logging.warning("⚠️  La variable d'environnement GOOGLE_API_KEY n'est pas définie. L'analyse fondamentale sera sautée.")
        else:
            analyzer = fundamental_analyzer.BRVMAnalyzer(spreadsheet_id=spreadsheet_id, api_key=google_api_key)
            analyzer.run_fundamental_analysis()
            logging.info("✅ Étape 2/3 (Analyse fondamentale) terminée avec succès.")

    except Exception as e:
        logging.error(f"❌ Échec à l'étape 2 (Analyse fondamentale): {e}", exc_info=True)
        # On continue même si cette étape échoue, car l'analyse technique peut quand même tourner.
    
    # --- Étape 3 : Analyse technique ---
    try:
        technical_analyzer.run_technical_analysis()
        logging.info("✅ Étape 3/3 (Analyse technique) terminée avec succès.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 (Analyse technique): {e}", exc_info=True)

    logging.info("🏁 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🏁")


if __name__ == "__main__":
    main()
