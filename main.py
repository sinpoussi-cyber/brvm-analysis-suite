# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - SUPABASE UNIQUEMENT (V4.1 FINAL)
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
    logging.info("🚀 BRVM ANALYSIS SUITE - SUPABASE UNIQUEMENT")
    logging.info("="*80)
    logging.info("Version: 4.1 Final (Corrections API Gemini + SQL)")
    logging.info("Base de données: Supabase (PostgreSQL)")
    logging.info("Modèle IA: gemini-1.5-flash-latest")
    logging.info("="*80)

    # --- Étape 1 : Collecte des données (SUPABASE) ---
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 1/5 : COLLECTE DES DONNÉES")
        logging.info("="*80)
        data_collector.run_data_collection()
        logging.info("✅ Données collectées et sauvegardées dans Supabase")
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 1 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 2 : Analyse technique (SUPABASE) ---
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 2/5 : ANALYSE TECHNIQUE")
        logging.info("="*80)
        technical_analyzer.run_technical_analysis()
        logging.info("✅ Analyses techniques calculées et sauvegardées dans Supabase")
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 2 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 3 : Prédictions (SUPABASE) ---
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 3/5 : GÉNÉRATION DES PRÉDICTIONS")
        logging.info("="*80)
        prediction_analyzer.run_prediction_analysis()
        logging.info("✅ Prédictions générées et sauvegardées dans Supabase")
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 3 : {e}", exc_info=True)
        sys.exit(1)

    # --- Étape 4 : Analyse fondamentale (SUPABASE) ---
    new_fundamental_analyses = []
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 4/5 : ANALYSE FONDAMENTALE (AVEC SYSTÈME DE MÉMOIRE)")
        logging.info("="*80)
        
        # Vérifier qu'au moins une clé API existe
        api_keys_available = any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 23))
        
        if api_keys_available:
            analyzer = fundamental_analyzer.BRVMAnalyzer()
            _, new_fundamental_analyses = analyzer.run_and_get_results()
            logging.info("✅ Analyses fondamentales générées et sauvegardées dans Supabase")
            logging.info(f"   📊 Nouvelles analyses : {len(new_fundamental_analyses)}")
        else:
            logging.warning("⚠️  Aucune clé API Gemini trouvée")
            logging.warning("   Étape 4 ignorée - Configurez GOOGLE_API_KEY_1 à GOOGLE_API_KEY_22")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 : {e}", exc_info=True)
        logging.info("   Passage à l'étape suivante...")

    # --- Étape 5 : Génération du rapport de synthèse ---
    db_connection = None
    try:
        logging.info("\n" + "="*80)
        logging.info("ÉTAPE 5/5 : GÉNÉRATION DES RAPPORTS")
        logging.info("="*80)
        
        # Vérifier qu'au moins une clé API existe
        api_keys_available = any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 23))
        
        if api_keys_available:
            DB_NAME = os.environ.get('DB_NAME')
            DB_USER = os.environ.get('DB_USER')
            DB_PASSWORD = os.environ.get('DB_PASSWORD')
            DB_HOST = os.environ.get('DB_HOST')
            DB_PORT = os.environ.get('DB_PORT')
            
            db_connection = psycopg2.connect(
                dbname=DB_NAME, 
                user=DB_USER, 
                password=DB_PASSWORD, 
                host=DB_HOST, 
                port=DB_PORT
            )
            
            final_report_generator = report_generator.ComprehensiveReportGenerator(db_connection)
            final_report_generator.generate_all_reports(new_fundamental_analyses)
            logging.info("✅ Rapports générés avec succès")
        else:
            logging.warning("⚠️  Aucune clé API Gemini trouvée")
            logging.warning("   Étape 5 ignorée - Configurez GOOGLE_API_KEY_1 à GOOGLE_API_KEY_22")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 5 : {e}", exc_info=True)
    finally:
        if db_connection:
            db_connection.close()

    # --- Résumé Final ---
    logging.info("\n" + "="*80)
    logging.info("🎉 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE")
    logging.info("="*80)
    logging.info("✅ Architecture : Supabase (PostgreSQL) uniquement")
    logging.info("✅ Modèle IA : gemini-1.5-flash-latest (API v1)")
    logging.info("📊 Tables mises à jour :")
    logging.info("   • companies (sociétés cotées)")
    logging.info("   • historical_data (données de marché)")
    logging.info("   • technical_analysis (indicateurs techniques)")
    logging.info("   • predictions (prédictions 20 jours)")
    logging.info("   • fundamental_analysis (analyses IA)")
    logging.info("")
    logging.info("📁 Fichiers générés :")
    logging.info("   • Rapport_Synthese_Investissement_BRVM_*.docx")
    logging.info("")
    logging.info("🔗 Accédez à vos données sur Supabase Dashboard")
    logging.info("="*80)

if __name__ == "__main__":
    main()
