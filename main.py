# ==============================================================================
# ORCHESTRATEUR PRINCIPAL - BRVM ANALYSIS SUITE (V2.0 - GESTION ROBUSTE)
# ==============================================================================
import os
import logging
import sys
import time
import json

# Importer les modules de chaque étape
import data_collector
import fundamental_analyzer
import technical_analyzer
import report_generator

# Configuration du logging améliorée
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('brvm_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def check_environment():
    """Vérification de l'environnement au démarrage"""
    required_vars = ['SPREADSHEET_ID', 'DRIVE_FOLDER_ID', 'GSPREAD_SERVICE_ACCOUNT']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logging.error(f"❌ Variables d'environnement manquantes : {missing_vars}")
        return False
    
    # Vérifier le JSON du compte de service
    try:
        json.loads(os.environ.get('GSPREAD_SERVICE_ACCOUNT'))
    except (json.JSONDecodeError, TypeError):
        logging.error("❌ GSPREAD_SERVICE_ACCOUNT n'est pas un JSON valide")
        return False
    
    # Compter les clés API disponibles
    api_key_count = sum(1 for i in range(1, 200) if os.environ.get(f'GOOGLE_API_KEY_{i}'))
    logging.info(f"📊 {api_key_count} clé(s) API Gemini détectées")
    
    return True

def execute_step_with_retry(step_name, step_function, max_retries=2, critical=True):
    """Exécute une étape avec retry automatique"""
    for attempt in range(max_retries):
        try:
            logging.info(f"{'='*60}")
            if attempt > 0:
                logging.info(f"ÉTAPE {step_name.upper()} - TENTATIVE {attempt + 1}/{max_retries}")
            else:
                logging.info(f"ÉTAPE {step_name.upper()}")
            logging.info(f"{'='*60}")
            
            result = step_function()
            logging.info(f"✅ Étape {step_name} terminée avec succès")
            return result, True
            
        except Exception as e:
            logging.error(f"❌ Erreur étape {step_name} (tentative {attempt + 1}): {e}")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 60  # 60, 120 secondes
                logging.warning(f"⏳ Nouvelle tentative dans {wait_time} secondes...")
                time.sleep(wait_time)
            else:
                if critical:
                    logging.error(f"❌ ÉCHEC CRITIQUE de l'étape {step_name} après {max_retries} tentatives")
                    return None, False
                else:
                    logging.warning(f"⚠️ Échec non-critique de l'étape {step_name} - Continuation...")
                    return None, True

def main():
    """Fonction principale avec gestion d'erreurs robuste"""
    logging.info("🚀 DÉMARRAGE DE LA SUITE D'ANALYSE BRVM COMPLÈTE 🚀")
    
    # Vérification de l'environnement
    if not check_environment():
        logging.error("❌ Problème de configuration - Arrêt du script")
        sys.exit(1)
    
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    drive_folder_id = os.environ.get('DRIVE_FOLDER_ID')
    
    # Assigner les IDs globalement
    data_collector.SPREADSHEET_ID = spreadsheet_id
    technical_analyzer.SPREADSHEET_ID = spreadsheet_id
    
    # État de progression
    completed_steps = []
    step_results = {}
    
    # --- Étape 1 : Collecte des données ---
    def run_data_collection():
        return data_collector.run_data_collection()
    
    result, success = execute_step_with_retry(
        "1/4 (Collecte de données)", 
        run_data_collection, 
        max_retries=2, 
        critical=False  # Non critique car on peut continuer avec les données existantes
    )
    
    if success:
        completed_steps.append("data_collection")
        step_results["data_collection"] = result
    
    # --- Étape 2 : Analyse technique ---
    def run_technical_analysis():
        return technical_analyzer.run_technical_analysis()
    
    result, success = execute_step_with_retry(
        "2/4 (Analyse technique)", 
        run_technical_analysis, 
        max_retries=2, 
        critical=False
    )
    
    if success:
        completed_steps.append("technical_analysis")
        step_results["technical_analysis"] = result
    
    # --- Étape 3 : Analyse fondamentale ---
    fundamental_results = {}
    new_fundamental_analyses = []
    
    # Compter les clés API disponibles
    api_key_count = sum(1 for i in range(1, 200) if os.environ.get(f'GOOGLE_API_KEY_{i}'))
    
    if api_key_count == 0:
        logging.warning("⚠️ Aucune clé API Gemini disponible - Étape 3 sautée")
    else:
        def run_fundamental_analysis():
            analyzer = fundamental_analyzer.BRVMAnalyzer(spreadsheet_id=spreadsheet_id)
            return analyzer.run_and_get_results()
        
        result, success = execute_step_with_retry(
            "3/4 (Analyse fondamentale)", 
            run_fundamental_analysis, 
            max_retries=1,  # Une seule tentative car c'est coûteux en API
            critical=False
        )
        
        if success and result:
            completed_steps.append("fundamental_analysis")
            fundamental_results, new_fundamental_analyses = result
            step_results["fundamental_analysis"] = result
    
    # --- Étape 4 : Génération des rapports ---
    if api_key_count > 0:
        def run_report_generation():
            final_report_generator = report_generator.ComprehensiveReportGenerator(
                spreadsheet_id=spreadsheet_id,
                drive_folder_id=drive_folder_id
            )
            return final_report_generator.generate_report(fundamental_results, new_fundamental_analyses)
        
        result, success = execute_step_with_retry(
            "4/4 (Génération des rapports)", 
            run_report_generation, 
            max_retries=2, 
            critical=False
        )
        
        if success:
            completed_steps.append("report_generation")
            step_results["report_generation"] = result
    else:
        logging.warning("⚠️ Étape 4 sautée - Aucune clé API disponible")
    
    # --- Résumé final ---
    logging.info("="*60)
    logging.info("📋 RÉSUMÉ D'EXÉCUTION")
    logging.info("="*60)
    
    total_steps = 4
    completed_count = len(completed_steps)
    
    logging.info(f"✅ Étapes complétées ({completed_count}/{total_steps}): {', '.join(completed_steps)}")
    
    if completed_count == 0:
        logging.error("❌ ÉCHEC TOTAL : Aucune étape n'a pu être complétée")
        sys.exit(1)
    elif completed_count < total_steps:
        logging.warning(f"⚠️ SUCCÈS PARTIEL : {completed_count}/{total_steps} étapes complétées")
        
        # Diagnostics pour les étapes échouées
        all_steps = ["data_collection", "technical_analysis", "fundamental_analysis", "report_generation"]
        failed_steps = [step for step in all_steps if step not in completed_steps]
        
        if failed_steps:
            logging.info("🔍 Étapes échouées:")
            for step in failed_steps:
                if step == "fundamental_analysis" and api_key_count == 0:
                    logging.info(f"   - {step}: Aucune clé API disponible")
                elif step == "report_generation" and api_key_count == 0:
                    logging.info(f"   - {step}: Aucune clé API disponible")
                else:
                    logging.info(f"   - {step}: Erreur technique")
    else:
        logging.info("🏁 SUCCÈS COMPLET : Toutes les étapes terminées avec succès")
    
    # Statistiques d'utilisation des API (si disponible)
    if completed_count > 0:
        logging.info("📊 Analyse terminée - Vérifiez les artifacts pour les rapports générés")

if __name__ == "__main__":
    main()
