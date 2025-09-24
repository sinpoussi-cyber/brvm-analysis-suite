# ==============================================================================
# MODULE: DATA COLLECTOR (V2.1 - DEBUGGING)
# ==============================================================================

import logging
import os
import psycopg2

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Récupération des Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

def connect_to_db():
    """Établit la connexion à la base de données PostgreSQL."""
    logging.info("Tentative de connexion à la base de données...")
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("✅ Connexion à la base de données PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Impossible de se connecter à la base de données PostgreSQL : {e}")
        return None

def run_data_collection():
    logging.info("="*60)
    logging.info("ÉTAPE 1 : DÉMARRAGE DU TEST DE COLLECTE DE DONNÉES (VERSION DEBUG)")
    logging.info("="*60)
    
    conn = connect_to_db()
    if not conn:
        logging.error("Arrêt du script car la connexion à la base de données a échoué.")
        return
        
    logging.info("Le script a réussi à se connecter et à s'exécuter.")
    
    cur = conn.cursor()
    cur.execute("SELECT symbol, id FROM companies LIMIT 5;")
    companies = cur.fetchall()
    logging.info(f"Test de lecture réussi. 5 premières sociétés : {companies}")
    
    cur.close()
    conn.close()
    logging.info("Processus de test de collecte de données terminé.")

if __name__ == "__main__":
    logging.info("Le script Python est bien lancé, exécution de main().")
    run_data_collection()
