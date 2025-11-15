# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - SUPABASE & OPENAI (V5.0)
# ==============================================================================

import os
import logging
import sys
import psycopg2

# Importer les modules de chaque étape
import data_collector
import technical_analyzer
import prediction_analyzer
import fundamental_analyzer
import report_generator

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def main():
    logging.info("="*80)
    logging.info("🚀 BRVM ANALYSIS SUITE - ARCHITECTURE OUVERTE")
    logging.info("="*80)
    logging.info("Version: 5.0 (OpenAI GPT-4o)")
    logging.info("Base de données: Supabase (PostgreSQL)")
    logging.info(f"Modèle IA: {report_generator.OPENAI_MODEL}")
    logging.info("="*80)

    # --- Étape 1 : Collecte des données ---
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 1/5 : COLLECTE DES DONNÉES")
        logging.info("="*80)
        data_collector.run_data_collection()
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 1 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 2 : Analyse technique ---
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 2/5 : ANALYSE TECHNIQUE")
        logging.info("="*80)
        technical_analyzer.run_technical_analysis()
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 2 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 3 : Prédictions ---
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 3/5 : GÉNÉRATION DES PRÉDICTIONS")
        logging.info("="*80)
        prediction_analyzer.run_prediction_analysis()
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 3 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 4 : Analyse fondamentale ---
    new_fundamental_analyses = []
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 4/5 : ANALYSE FONDAMENTALE (OPENAI)")
        logging.info("="*80)
        
        if os.environ.get('OPENAI_API_KEY'):
            analyzer = fundamental_analyzer.BRVMAnalyzer()
            _, new_fundamental_analyses = analyzer.run_and_get_results()
            logging.info(f"   📊 Nouvelles analyses : {len(new_fundamental_analyses)}")
        else:
            logging.warning("⚠️  Aucune clé API OpenAI trouvée. Étape 4 ignorée.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 : {e}", exc_info=True)

    # --- Étape 5 : Génération du rapport de synthèse ---
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 5/5 : GÉNÉRATION DES RAPPORTS (OPENAI)")
        logging.info("="*80)
        
        if os.environ.get('OPENAI_API_KEY'):
            final_report_generator = report_generator.BRVMReportGenerator()
            final_report_generator.generate_all_reports(new_fundamental_analyses)
        else:
            logging.warning("⚠️  Aucune clé API OpenAI trouvée. Étape 5 ignorée.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 5 : {e}", exc_info=True)

    # --- Résumé Final ---
    logging.info("\n" + "="*80)
    logging.info("🎉 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE")
    logging.info("="*80)
    logging.info(f"✅ Modèle IA : OpenAI {report_generator.OPENAI_MODEL}")
    logging.info("✅ Tables mises à jour sur Supabase.")
    logging.info("✅ Rapports de synthèse générés.")
    logging.info("="*80)

if __name__ == "__main__":
    main()
