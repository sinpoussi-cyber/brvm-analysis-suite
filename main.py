# ==============================================================================
# BRVM ANALYSIS SUITE - MAIN ORCHESTRATOR
# ==============================================================================

import logging
from data_collector import BRVMDataCollector
from technical_analyzer import TechnicalAnalyzer
from prediction_analyzer import PredictionAnalyzer
from fundamental_analyzer import BRVMAnalyzer
from report_generator import BRVMReportGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


def main():
    """Orchestrateur principal de la suite d'analyse BRVM"""
    
    logging.info("="*80)
    logging.info("🚀 BRVM ANALYSIS SUITE - DÉMARRAGE")
    logging.info("="*80)
    
    try:
        # ÉTAPE 1: Collecte des données
        logging.info("\n📊 ÉTAPE 1: COLLECTE DES DONNÉES")
        collector = BRVMDataCollector()
        collector.run()
        
        # ÉTAPE 2: Analyse technique
        logging.info("\n📈 ÉTAPE 2: ANALYSE TECHNIQUE")
        tech_analyzer = TechnicalAnalyzer()
        tech_analyzer.run()
        
        # ÉTAPE 3: Prédictions
        logging.info("\n🔮 ÉTAPE 3: PRÉDICTIONS")
        pred_analyzer = PredictionAnalyzer()
        pred_analyzer.run()
        
        # ÉTAPE 4: Analyse fondamentale
        logging.info("\n📄 ÉTAPE 4: ANALYSE FONDAMENTALE")
        fund_analyzer = BRVMAnalyzer()
        fundamental_results, new_analyses = fund_analyzer.run_and_get_results()
        
        # ÉTAPE 5: Génération des rapports
        logging.info("\n📝 ÉTAPE 5: GÉNÉRATION DES RAPPORTS")
        report_gen = BRVMReportGenerator()
        report_gen.generate_all_reports(new_analyses)
        
        logging.info("\n" + "="*80)
        logging.info("✅ BRVM ANALYSIS SUITE - TERMINÉ AVEC SUCCÈS")
        logging.info("="*80)
        
    except Exception as e:
        logging.critical(f"\n❌ ERREUR CRITIQUE: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
