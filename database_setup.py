# ==============================================================================
# MODULE: DATABASE SETUP (V1.2 - COMPLET)
# ==============================================================================

import psycopg2
from psycopg2 import sql
import os
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Récupérer les identifiants depuis les secrets GitHub
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

def setup_tables():
    """
    Connecte à la base de données PostgreSQL en ligne et crée toutes les tables nécessaires.
    """
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        logging.error("❌ Un ou plusieurs secrets de base de données (DB_NAME, DB_USER, etc.) sont manquants.")
        return

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        logging.info(f"✅ Connecté à la base de données sur '{DB_HOST}' pour la configuration des tables.")

        # Création de la table 'companies'
        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                brvm_url TEXT
            );
        """)
        logging.info("Table 'companies' vérifiée/créée.")

        # Création de la table 'historical_data'
        cur.execute("""
            CREATE TABLE IF NOT EXISTS historical_data (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                trade_date DATE NOT NULL,
                price NUMERIC(10, 3),
                volume BIGINT,
                value NUMERIC(15, 3),
                UNIQUE (company_id, trade_date)
            );
        """)
        logging.info("Table 'historical_data' vérifiée/créée.")

        # Création de la table 'technical_analysis'
        cur.execute("""
            CREATE TABLE IF NOT EXISTS technical_analysis (
                id SERIAL PRIMARY KEY,
                historical_data_id INTEGER UNIQUE NOT NULL REFERENCES historical_data(id) ON DELETE CASCADE,
                mm5 NUMERIC(10, 3),
                mm10 NUMERIC(10, 3),
                mm20 NUMERIC(10, 3),
                mm50 NUMERIC(10, 3),
                mm_decision VARCHAR(50),
                bollinger_central NUMERIC(10, 3),
                bollinger_inferior NUMERIC(10, 3),
                bollinger_superior NUMERIC(10, 3),
                bollinger_decision VARCHAR(50),
                macd_line NUMERIC(10, 3),
                signal_line NUMERIC(10, 3),
                histogram NUMERIC(10, 3),
                macd_decision VARCHAR(50),
                rsi NUMERIC(10, 3),
                rsi_decision VARCHAR(50),
                stochastic_k NUMERIC(10, 3),
                stochastic_d NUMERIC(10, 3),
                stochastic_decision VARCHAR(50)
            );
        """)
        logging.info("Table 'technical_analysis' vérifiée/créée.")

        # Création de la table 'fundamental_analysis'
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fundamental_analysis (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                report_url TEXT UNIQUE NOT NULL,
                report_title TEXT,
                report_date DATE,
                analysis_summary TEXT,
                analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("Table 'fundamental_analysis' vérifiée/créée.")
        
        # Création de la table 'new_market_events'
        cur.execute("""
            CREATE TABLE IF NOT EXISTS new_market_events (
                id SERIAL PRIMARY KEY,
                event_date DATE NOT NULL DEFAULT CURRENT_DATE,
                event_summary TEXT NOT NULL,
                creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("Table 'new_market_events' vérifiée/créée.")

        conn.commit()
        logging.info("✅ Toutes les tables ont été vérifiées/créées avec succès.")

    except psycopg2.Error as e:
        logging.error(f"❌ Erreur PostgreSQL: {e}")
        if conn: conn.rollback()
    except Exception as e:
        logging.error(f"❌ Erreur inattendue: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__ == "__main__":
    logging.info("🚀 Démarrage de la configuration de la base de données BRVM.")
    setup_tables()
    logging.info("🏁 Fin de la configuration de la base de données.")
