# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - ARCHITECTURE POSTGRESQL (V2.3 - VERSION CORRIGÉE)
# ==============================================================================

import os
import logging
import sys
import time
import psycopg2

# Importer les modules de chaque étape
import data_collector
import technical_analyzer
import fundamental_analyzer
import report_generator
import export_to_gsheet

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    start_time = time.time()
    
    logging.info("=" * 80)
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE (ARCHITECTURE DB) 🚀")
    logging.info("=" * 80)
    
    # Vérification des secrets critiques
    required_secrets = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']
    missing_secrets = [secret for secret in required_secrets if not os.environ.get(secret)]
    
    if missing_secrets:
        logging.critical(f"❌ Secrets manquants : {', '.join(missing_secrets)}")
        logging.critical("Impossible de continuer sans connexion à la base de données.")
        sys.exit(1)
    
    logging.info(f"✅ Tous les secrets critiques sont présents")
    
    # Statistiques d'exécution
    stats = {
        'data_collected': False,
        'technical_analyzed': False,
        'fundamental_analyzed': False,
        'reports_generated': False,
        'exported_to_gsheet': False,
        'new_fundamental_count': 0
    }

    # --- ÉTAPE 1 : Collecte des données ---
    try:
        logging.info("\n" + "=" * 80)
        logging.info("📥 ÉTAPE 1/5 : COLLECTE DES DONNÉES DE MARCHÉ")
        logging.info("=" * 80)
        data_collector.run_data_collection()
        stats['data_collected'] = True
        logging.info("✅ Étape 1 terminée avec succès")
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 1 : {e}", exc_info=True)
        sys.exit(1)

    # --- ÉTAPE 2 : Analyse technique ---
    try:
        logging.info("\n" + "=" * 80)
        logging.info("📊 ÉTAPE 2/5 : ANALYSE TECHNIQUE")
        logging.info("=" * 80)
        technical_analyzer.run_technical_analysis()
        stats['technical_analyzed'] = True
        logging.info("✅ Étape 2 terminée avec succès")
    except Exception as e:
        logging.critical(f"❌ Échec critique à l'étape 2 : {e}", exc_info=True)
        sys.exit(1)

    # --- ÉTAPE 3 : Analyse fondamentale ---
    new_fundamental_analyses = []
    try:
        logging.info("\n" + "=" * 80)
        logging.info("🔍 ÉTAPE 3/5 : ANALYSE FONDAMENTALE (IA)")
        logging.info("=" * 80)
        
        # Vérifier si au moins une clé API Gemini est présente
        has_api_key = any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20))
        
        if has_api_key:
            analyzer = fundamental_analyzer.BRVMAnalyzer()
            _, new_fundamental_analyses = analyzer.run_and_get_results()
            stats['fundamental_analyzed'] = True
            stats['new_fundamental_count'] = len(new_fundamental_analyses)
            logging.info(f"✅ Étape 3 terminée : {len(new_fundamental_analyses)} nouvelles analyses")
        else:
            logging.warning("⚠️ Aucune clé API Gemini trouvée. Analyse fondamentale ignorée.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 3 : {e}", exc_info=True)
        logging.info("⚠️ Poursuite du workflow malgré l'erreur...")

    # --- ÉTAPE 4 : Génération des rapports ---
    db_connection = None
    try:
        logging.info("\n" + "=" * 80)
        logging.info("📝 ÉTAPE 4/5 : GÉNÉRATION DES RAPPORTS")
        logging.info("=" * 80)
        
        # Vérifier si on a des clés API pour générer les rapports
        has_api_key = any(os.environ.get(f'GOOGLE_API_KEY_{i}') for i in range(1, 20))
        
        if has_api_key:
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
            stats['reports_generated'] = True
            logging.info("✅ Étape 4 terminée avec succès")
        else:
            logging.warning("⚠️ Aucune clé API Gemini. Génération de rapports ignorée.")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 4 : {e}", exc_info=True)
    finally:
        if db_connection and not db_connection.closed:
            db_connection.close()

    # --- ÉTAPE 5 : Export vers Google Sheets (OPTIONNEL) ---
    try:
        logging.info("\n" + "=" * 80)
        logging.info("📤 ÉTAPE 5/5 : EXPORT VERS GOOGLE SHEETS (OPTIONNEL)")
        logging.info("=" * 80)
        
        if os.environ.get('SPREADSHEET_ID') and os.environ.get('GSPREAD_SERVICE_ACCOUNT'):
            export_to_gsheet.export_today_data()
            stats['exported_to_gsheet'] = True
            logging.info("✅ Étape 5 terminée avec succès")
        else:
            logging.warning("⚠️ Secrets Google Sheets manquants. Export ignoré (normal si non configuré).")
    except Exception as e:
        logging.error(f"❌ Échec à l'étape 5 : {e}", exc_info=True)
        logging.warning("⚠️ Export Google Sheets échoué, mais poursuite du workflow...")

    # --- RÉSUMÉ FINAL ---
    end_time = time.time()
    duration = end_time - start_time
    
    logging.info("\n" + "=" * 80)
    logging.info("📊 RÉSUMÉ DE L'EXÉCUTION")
    logging.info("=" * 80)
    logging.info(f"⏱️  Durée totale : {duration:.2f} secondes ({duration/60:.2f} minutes)")
    logging.info(f"📥 Collecte de données : {'✅' if stats['data_collected'] else '❌'}")
    logging.info(f"📊 Analyse technique : {'✅' if stats['technical_analyzed'] else '❌'}")
    logging.info(f"🔍 Analyse fondamentale : {'✅' if stats['fundamental_analyzed'] else '❌'}")
    logging.info(f"   └─ Nouvelles analyses IA : {stats['new_fundamental_count']}")
    logging.info(f"📝 Génération de rapports : {'✅' if stats['reports_generated'] else '❌'}")
    logging.info(f"📤 Export Google Sheets : {'✅' if stats['exported_to_gsheet'] else '❌'}")
    logging.info("=" * 80)
    logging.info("🎉 SUITE D'ANALYSE BRVM COMPLÈTE TERMINÉE 🎉")
    logging.info("=" * 80)

if __name__ == "__main__":
    main()
