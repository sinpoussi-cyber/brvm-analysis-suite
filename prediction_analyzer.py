# ==============================================================================
# MODULE: PREDICTION ANALYZER - ALGORITHME HYBRIDE
# Prédiction sur 20 jours ouvrés (Mardi-Samedi)
# ==============================================================================

import psycopg2
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import os
import logging
import json
import gspread
from google.oauth2 import service_account
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
GSPREAD_SERVICE_ACCOUNT_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# --- Connexion PostgreSQL ---
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        logging.info("✅ Connexion PostgreSQL réussie.")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur connexion DB: {e}")
        return None

# --- Authentification Google Sheets ---
def authenticate_gsheets():
    try:
        if not GSPREAD_SERVICE_ACCOUNT_JSON:
            return None
        
        creds_dict = json.loads(GSPREAD_SERVICE_ACCOUNT_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        logging.info("✅ Authentification Google Sheets réussie.")
        return gc
    except Exception as e:
        logging.error(f"❌ Erreur authentification Google Sheets: {e}")
        return None

# --- Génération des jours ouvrés (Mardi-Samedi) ---
def generate_business_days(start_date, num_days=20):
    """
    Génère les 20 prochains jours ouvrés (Mardi-Samedi)
    La BRVM est ouverte du Mardi au Samedi
    """
    business_days = []
    current_date = start_date + timedelta(days=1)
    
    while len(business_days) < num_days:
        # 0=Lundi, 1=Mardi, 2=Mercredi, 3=Jeudi, 4=Vendredi, 5=Samedi, 6=Dimanche
        weekday = current_date.weekday()
        
        # Mardi(1) au Samedi(5)
        if 1 <= weekday <= 5:
            business_days.append(current_date)
        
        current_date += timedelta(days=1)
    
    return business_days

# --- Algorithme Hybride de Prédiction ---
def hybrid_prediction(prices, dates):
    """
    Algorithme hybride combinant 3 méthodes :
    1. Régression Linéaire (40%)
    2. Tendance Récente (30%)
    3. Moyenne Mobile Pondérée (30%)
    """
    if len(prices) < 100:
        logging.warning("Pas assez de données pour prédiction (< 100 jours)")
        return None
    
    # Prendre les 100 derniers jours
    prices_100 = prices[-100:].values
    dates_100 = dates[-100:]
    
    # Convertir les dates en nombres (jours depuis le début)
    days = np.arange(len(prices_100)).reshape(-1, 1)
    
    # Dernière valeur connue
    last_price = prices_100[-1]
    
    # Générer les 20 prochains jours ouvrés
    last_date = dates_100.iloc[-1].date()
    future_business_days = generate_business_days(last_date, num_days=20)
    future_days = np.arange(len(prices_100), len(prices_100) + 20).reshape(-1, 1)
    
    # ═══════════════════════════════════════════════════════════
    # MÉTHODE 1 : RÉGRESSION LINÉAIRE (40%)
    # ═══════════════════════════════════════════════════════════
    model = LinearRegression()
    model.fit(days, prices_100)
    prediction_linear = model.predict(future_days).flatten()
    
    # ═══════════════════════════════════════════════════════════
    # MÉTHODE 2 : TENDANCE RÉCENTE (30%)
    # Basée sur les 30 derniers jours
    # ═══════════════════════════════════════════════════════════
    prices_30 = prices_100[-30:]
    trend_recent = (prices_30[-1] - prices_30[0]) / 30
    prediction_trend = np.array([last_price + trend_recent * (i + 1) for i in range(20)])
    
    # ═══════════════════════════════════════════════════════════
    # MÉTHODE 3 : MOYENNE MOBILE PONDÉRÉE (30%)
    # Poids exponentiels (plus récent = plus important)
    # ═══════════════════════════════════════════════════════════
    weights = np.exp(np.linspace(-1, 0, 30))
    weights = weights / weights.sum()  # Normaliser
    weighted_avg = np.average(prices_30, weights=weights)
    
    # Extrapoler avec la moyenne pondérée
    drift = weighted_avg - prices_30[0]
    prediction_weighted = np.array([weighted_avg + drift * (i + 1) / 30 for i in range(20)])
    
    # ═══════════════════════════════════════════════════════════
    # PRÉDICTION FINALE (Combinaison pondérée)
    # ═══════════════════════════════════════════════════════════
    prediction_final = (
        0.4 * prediction_linear +
        0.3 * prediction_trend +
        0.3 * prediction_weighted
    )
    
    # Calculer l'intervalle de confiance (± 5%)
    confidence_interval = prediction_final * 0.05
    lower_bound = prediction_final - confidence_interval
    upper_bound = prediction_final + confidence_interval
    
    # Calculer la variation moyenne prévue
    avg_change = (prediction_final[-1] - last_price) / last_price * 100
    
    return {
        'dates': future_business_days,
        'predictions': prediction_final,
        'lower_bound': lower_bound,
        'upper_bound': upper_bound,
        'last_price': last_price,
        'avg_change_percent': avg_change,
        'confidence': 'Moyenne' if abs(avg_change) < 5 else 'Faible' if abs(avg_change) > 10 else 'Élevée'
    }

# --- Sauvegarde dans PostgreSQL ---
def save_predictions_to_db(conn, company_id, symbol, prediction_data):
    """Sauvegarde les prédictions dans la table predictions"""
    try:
        with conn.cursor() as cur:
            # Supprimer les anciennes prédictions pour cette société
            cur.execute("DELETE FROM predictions WHERE company_id = %s", (company_id,))
            
            # Insérer les nouvelles prédictions
            for i, pred_date in enumerate(prediction_data['dates']):
                cur.execute("""
                    INSERT INTO predictions (
                        company_id, 
                        prediction_date, 
                        predicted_price, 
                        lower_bound, 
                        upper_bound,
                        confidence_level,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    company_id,
                    pred_date,
                    float(prediction_data['predictions'][i]),
                    float(prediction_data['lower_bound'][i]),
                    float(prediction_data['upper_bound'][i]),
                    prediction_data['confidence']
                ))
            
            conn.commit()
            logging.info(f"   ✅ PostgreSQL: 20 prédictions sauvegardées pour {symbol}")
            return True
    
    except Exception as e:
        logging.error(f"❌ Erreur sauvegarde prédictions DB pour {symbol}: {e}")
        conn.rollback()
        return False

# --- Sauvegarde dans Google Sheets ---
def save_predictions_to_gsheet(gc, spreadsheet, symbol, prediction_data):
    """Sauvegarde les prédictions dans une feuille dédiée"""
    try:
        sheet_name = f"{symbol}_Predictions"
        
        # Créer ou ouvrir la feuille
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            # Effacer le contenu existant
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=30, cols=6)
        
        # En-têtes
        headers = ['Date', 'Prix Prédit', 'Borne Inférieure', 'Borne Supérieure', 'Confiance', 'Variation %']
        
        # Données
        rows = [headers]
        for i, pred_date in enumerate(prediction_data['dates']):
            variation = ((prediction_data['predictions'][i] - prediction_data['last_price']) / 
                        prediction_data['last_price'] * 100)
            
            rows.append([
                pred_date.strftime('%d/%m/%Y'),
                f"{prediction_data['predictions'][i]:.2f}",
                f"{prediction_data['lower_bound'][i]:.2f}",
                f"{prediction_data['upper_bound'][i]:.2f}",
                prediction_data['confidence'],
                f"{variation:.2f}%"
            ])
        
        # Écrire toutes les données
        worksheet.update('A1', rows, value_input_option='USER_ENTERED')
        
        logging.info(f"   ✅ Google Sheets: feuille {sheet_name} mise à jour")
        return True
    
    except Exception as e:
        logging.error(f"❌ Erreur sauvegarde GSheet prédictions pour {symbol}: {e}")
        return False

# --- Traitement par société ---
def process_company_prediction(conn, gc, spreadsheet, company_id, symbol):
    """Génère et sauvegarde les prédictions pour une société"""
    logging.info(f"--- Prédiction: {symbol} ---")
    
    try:
        # Récupérer les 100 derniers jours de données
        query = """
            SELECT trade_date, price 
            FROM historical_data 
            WHERE company_id = %s 
            ORDER BY trade_date DESC 
            LIMIT 100
        """
        df = pd.read_sql(query, conn, params=(company_id,))
        
        if len(df) < 100:
            logging.warning(f"   ⚠️  Pas assez de données ({len(df)} jours < 100)")
            return False
        
        # Inverser pour avoir du plus ancien au plus récent
        df = df.iloc[::-1].reset_index(drop=True)
        
        # Générer les prédictions
        prediction_data = hybrid_prediction(df['price'], df['trade_date'])
        
        if prediction_data is None:
            return False
        
        # Afficher un résumé
        logging.info(f"   📊 Prix actuel: {prediction_data['last_price']:.2f} F CFA")
        logging.info(f"   📊 Prix prédit J+20: {prediction_data['predictions'][-1]:.2f} F CFA")
        logging.info(f"   📊 Variation prévue: {prediction_data['avg_change_percent']:.2f}%")
        logging.info(f"   📊 Confiance: {prediction_data['confidence']}")
        
        # Sauvegarder dans PostgreSQL
        save_predictions_to_db(conn, company_id, symbol, prediction_data)
        
        # Sauvegarder dans Google Sheets
        if gc and spreadsheet:
            save_predictions_to_gsheet(gc, spreadsheet, symbol, prediction_data)
        
        return True
    
    except Exception as e:
        logging.error(f"❌ Erreur prédiction {symbol}: {e}")
        return False

# --- Fonction principale ---
def run_prediction_analysis():
    logging.info("="*80)
    logging.info("🔮 ÉTAPE 3: PRÉDICTIONS (ALGORITHME HYBRIDE - 20 JOURS OUVRÉS)")
    logging.info("="*80)
    
    conn = connect_to_db()
    if not conn:
        return
    
    gc = authenticate_gsheets()
    spreadsheet = None
    
    if gc:
        try:
            spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        except Exception as e:
            logging.error(f"❌ Erreur ouverture spreadsheet: {e}")
    
    try:
        # Récupérer toutes les sociétés
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies ORDER BY symbol;")
            companies = cur.fetchall()
        
        logging.info(f"📊 {len(companies)} société(s) à analyser")
        
        success_count = 0
        for company_id, symbol in companies:
            if process_company_prediction(conn, gc, spreadsheet, company_id, symbol):
                success_count += 1
        
        logging.info("\n" + "="*80)
        logging.info(f"✅ Prédictions terminées")
        logging.info(f"📊 Succès: {success_count}/{len(companies)} sociétés")
        logging.info("="*80)
    
    except Exception as e:
        logging.error(f"❌ Erreur critique: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_prediction_analysis()
