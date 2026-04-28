# ==============================================================================
# BRVM ANALYSIS SUITE - MAIN ORCHESTRATOR
# Version enrichie avec collecte macro-économique internationale (RSS)
# ==============================================================================
#
# ORDRE D'EXÉCUTION :
#   Étape 0 : MacroCollector      — Collecte actualités RSS + enrichissement IA
#   Étape 1 : BRVMDataCollector   — Collecte cours & indicateurs BRVM
#   Étape 2 : TechnicalAnalyzer   — Calcul indicateurs techniques
#   Étape 3 : PredictionAnalyzer  — Prédictions ML (GRU/LSTM)
#   Étape 4 : BRVMAnalyzer        — Analyse fondamentale Multi-AI
#   Étape 5 : BRVMReportGenerator — Génération rapport Word
#
# VARIABLES D'ENVIRONNEMENT REQUISES (GitHub Secrets) :
#   DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
#   DEEPSEEK_API_KEY, GEMINI_API_KEY, MISTRAL_API_KEY
#
# CORRECTIONS v2 :
#   - Stacktrace complète loggée pour chaque étape échouée
#   - Correction scope db_conn_macro (était non défini dans le finally)
#   - Étape 0 loggue désormais l'exception complète pour faciliter le debug
# ==============================================================================

import logging
import os
import sys
import time
import traceback
import psycopg2

from data_collector       import BRVMDataCollector
from technical_analyzer   import TechnicalAnalyzer
from prediction_analyzer  import PredictionAnalyzer
from fundamental_analyzer import BRVMAnalyzer
from report_generator     import BRVMReportGenerator
from macro_collector      import MacroCollector

# ── Configuration du logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

# ── Lecture des credentials depuis l'environnement ───────────────────────────
DB_NAME      = os.environ.get("DB_NAME")
DB_USER      = os.environ.get("DB_USER")
DB_PASSWORD  = os.environ.get("DB_PASSWORD")
DB_HOST      = os.environ.get("DB_HOST")
DB_PORT      = os.environ.get("DB_PORT", "5432")

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
MISTRAL_API_KEY  = os.environ.get("MISTRAL_API_KEY")

# Support de plusieurs clés Gemini pour la rotation
GEMINI_API_KEYS = [
    k for k in [
        os.environ.get("GEMINI_API_KEY"),
        os.environ.get("GEMINI_API_KEY_2"),
        os.environ.get("GEMINI_API_KEY_3"),
    ]
    if k
]


# ==============================================================================
# UTILITAIRES
# ==============================================================================

def _get_db_connection():
    """Crée et retourne une connexion PostgreSQL (Supabase)."""
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST]):
        raise EnvironmentError(
            "Variables DB manquantes : DB_NAME, DB_USER, DB_PASSWORD, DB_HOST"
        )
    return psycopg2.connect(
        dbname   = DB_NAME,
        user     = DB_USER,
        password = DB_PASSWORD,
        host     = DB_HOST,
        port     = DB_PORT,
        connect_timeout = 30,
        options  = "-c statement_timeout=300000",   # 5 min max par requête
    )


def _log_step(numero: int, emoji: str, titre: str):
    logging.info("")
    logging.info("=" * 70)
    logging.info(f"{emoji}  ÉTAPE {numero} : {titre}")
    logging.info("=" * 70)


def _log_success(message: str):
    logging.info(f"✅  {message}")


def _log_warning(message: str):
    logging.warning(f"⚠️   {message}")


def _log_error(message: str):
    logging.error(f"❌  {message}")


# ==============================================================================
# ORCHESTRATEUR PRINCIPAL
# ==============================================================================

def main():
    """
    Orchestrateur principal de la suite d'analyse BRVM.

    En cas d'échec d'une étape non critique (Macro, Technique, Prédictions),
    l'exécution continue pour garantir qu'un rapport est toujours généré.
    En cas d'échec d'une étape critique (Collecte données, Rapport), on lève.
    """

    start_time = time.time()

    logging.info("=" * 70)
    logging.info("🚀  BRVM ANALYSIS SUITE — DÉMARRAGE")
    logging.info(f"    APIs disponibles :"
                 f" Gemini×{len(GEMINI_API_KEYS)}"
                 f" | DeepSeek={'✅' if DEEPSEEK_API_KEY else '❌'}"
                 f" | Mistral={'✅' if MISTRAL_API_KEY else '❌'}")
    logging.info("=" * 70)

    new_analyses = {}   # résultats fondamentaux transmis au rapport

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 0 — Collecte actualités macro-économiques internationales (RSS)
    # Non critique : si elle échoue, le rapport se génère quand même
    # ──────────────────────────────────────────────────────────────────────────
    _log_step(0, "🌍", "COLLECTE ACTUALITÉS MACRO (RSS Google News + IA)")

    # CORRECTION : initialiser db_conn_macro avant le try pour éviter
    # NameError dans le finally si la connexion échoue
    db_conn_macro = None
    try:
        db_conn_macro = _get_db_connection()
        macro_collector = MacroCollector(
            db_conn                 = db_conn_macro,
            gemini_keys             = GEMINI_API_KEYS,
            deepseek_key            = DEEPSEEK_API_KEY,
            mistral_key             = MISTRAL_API_KEY,
            max_articles_per_source = 10,
        )
        macro_stats = macro_collector.run()
        _log_success(
            f"Macro collecté — Insérés: {macro_stats.get('inserted', 0)} | "
            f"Ignorés: {macro_stats.get('skipped', 0)} | "
            f"Erreurs: {macro_stats.get('errors', 0)}"
        )
    except Exception as e:
        # CORRECTION : afficher la stacktrace complète pour faciliter le debug
        _log_warning(f"Étape 0 échouée (non critique) : {e}")
        logging.warning("    Stacktrace complète :")
        logging.warning(traceback.format_exc())
        logging.info("    → Le rapport sera généré sans nouvelles actualités macro RSS")
    finally:
        if db_conn_macro:
            try:
                db_conn_macro.close()
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 1 — Collecte des données de marché BRVM
    # Critique : sans données fraîches, le rapport est invalide
    # ──────────────────────────────────────────────────────────────────────────
    _log_step(1, "📊", "COLLECTE DES DONNÉES BRVM")

    try:
        collector = BRVMDataCollector()
        collector.run()
        _log_success("Collecte données BRVM terminée")
    except Exception as e:
        _log_error(f"Étape 1 CRITIQUE : {e}")
        logging.error(traceback.format_exc())
        raise RuntimeError(f"Collecte BRVM échouée : {e}") from e

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 2 — Analyse technique (MM, Bollinger, MACD, RSI, Stoch)
    # Non critique
    # ──────────────────────────────────────────────────────────────────────────
    _log_step(2, "📈", "ANALYSE TECHNIQUE")

    try:
        tech_analyzer = TechnicalAnalyzer()
        tech_analyzer.run()
        _log_success("Analyse technique terminée")
    except Exception as e:
        _log_warning(f"Étape 2 échouée (non critique) : {e}")
        logging.warning(traceback.format_exc())
        logging.info("    → Signaux techniques manquants dans le rapport")

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 3 — Prédictions ML (GRU / LSTM / BiGRU)
    # Non critique
    # ──────────────────────────────────────────────────────────────────────────
    _log_step(3, "🔮", "PRÉDICTIONS ML (GRU/LSTM)")

    try:
        pred_analyzer = PredictionAnalyzer()
        pred_analyzer.run()
        _log_success("Prédictions ML terminées")
    except Exception as e:
        _log_warning(f"Étape 3 échouée (non critique) : {e}")
        logging.warning(traceback.format_exc())
        logging.info("    → Tableaux de prédictions absents dans le rapport")

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 4 — Analyse fondamentale Multi-AI
    # Non critique
    # ──────────────────────────────────────────────────────────────────────────
    _log_step(4, "📄", "ANALYSE FONDAMENTALE MULTI-AI")

    try:
        fund_analyzer = BRVMAnalyzer()
        fundamental_results, new_analyses = fund_analyzer.run_and_get_results()
        _log_success(
            f"Analyse fondamentale terminée — "
            f"{len(new_analyses) if new_analyses else 0} nouvelles analyse(s)"
        )
    except Exception as e:
        _log_warning(f"Étape 4 échouée (non critique) : {e}")
        logging.warning(traceback.format_exc())
        logging.info("    → Le rapport utilisera les analyses fondamentales existantes en BDD")
        new_analyses = {}

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 5 — Génération du rapport Word
    # Critique : c'est le livrable final
    # ──────────────────────────────────────────────────────────────────────────
    _log_step(5, "📝", "GÉNÉRATION DU RAPPORT WORD")

    try:
        report_gen = BRVMReportGenerator()
        report_gen.generate_all_reports(new_analyses)
        _log_success("Rapport Word généré avec succès")
    except Exception as e:
        _log_error(f"Étape 5 CRITIQUE : {e}")
        logging.error(traceback.format_exc())
        raise RuntimeError(f"Génération rapport échouée : {e}") from e

    # ──────────────────────────────────────────────────────────────────────────
    # RÉSUMÉ FINAL
    # ──────────────────────────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    logging.info("")
    logging.info("=" * 70)
    logging.info("✅  BRVM ANALYSIS SUITE — TERMINÉ AVEC SUCCÈS")
    logging.info(f"    Durée totale : {minutes}m {seconds}s")
    logging.info("=" * 70)


# ==============================================================================
# POINT D'ENTRÉE
# ==============================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        _log_error(f"ERREUR FATALE : {e}")
        logging.critical("Stacktrace complète :", exc_info=True)
        sys.exit(1)
