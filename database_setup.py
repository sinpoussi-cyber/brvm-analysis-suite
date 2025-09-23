# ==============================================================================
# MODULE: DATABASE SETUP (V1.1 - POUR GITHUB ACTIONS)
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
    Connecte à la base de données PostgreSQL en ligne et crée les tables.
    """
    # Vérifier que tous les secrets sont présents
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

        # ... (le reste des commandes CREATE TABLE est identique à la version précédente) ...
        # (Copiez-collez les commandes CREATE TABLE pour historical_data, technical_analysis, etc. ici)

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
