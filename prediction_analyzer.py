# ==============================================================================
# MODULE: PREDICTION ANALYZER V9.0 - PRÉDICTIONS JOUR PAR JOUR (20 JOURS)
# ==============================================================================

import psycopg2
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# --- Configuration & Secrets ---
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        logging.info("✅ Connexion PostgreSQL réussie")
        return conn
    except Exception as e:
        logging.error(f"❌ Erreur connexion DB: {e}")
        return None

def generate_business_days(start_date, num_days=20):
    """Génère les 20 prochains jours ouvrables (Lun-Ven)"""
    business_days = []
    current_date = start_date + timedelta(days=1)
    
    while len(business_days) < num_days:
        if 0 <= current_date.weekday() <= 4:  # Lun-Ven
            business_days.append(current_date)
        current_date += timedelta(days=1)
    
    return business_days

def hybrid_prediction_daily(prices, dates):
    """
    Algorithme hybride générant une prédiction POUR CHAQUE jour
    
    Returns:
        dict: 20 prédictions avec dates, prix, fourchettes, confiance
    """
    if len(prices) < 100:
        logging.warning("Pas assez de données (< 100 jours)")
        return None
    
    prices_100 = prices[-100:].values
    dates_100 = dates[-100:]
    
    days = np.arange(len(prices_100)).reshape(-1, 1)
    last_price = prices_100[-1]
    
    last_date_value = dates_100.iloc[-1]
    if isinstance(last_date_value, datetime):
        last_date = last_date_value.date()
    else:
        last_date = last_date_value
    
    future_business_days = generate_business_days(last_date, num_days=20)
    
    daily_predictions = []
    daily_lower_bounds = []
    daily_upper_bounds = []
    daily_confidence = []
    
    for day_offset in range(1, 21):
        future_day = np.array([[len(prices_100) + day_offset - 1]])
        
        # MÉTHODE 1 : Régression Linéaire (40%)
        model = LinearRegression()
        model.fit(days, prices_100)
        pred_linear = model.predict(future_day)[0]
        
        # MÉTHODE 2 : Tendance Récente (30%)
        prices_30 = prices_100[-30:]
        trend = (prices_30[-1] - prices_30[0]) / 30
        pred_trend = last_price + (trend * day_offset)
        
        # MÉTHODE 3 : Moyenne Mobile Pondérée (30%)
        weights = np.exp(np.linspace(-1, 0, 30))
        weights = weights / weights.sum()
        weighted_avg = np.average(prices_30, weights=weights)
        drift = (weighted_avg - prices_30[0]) / 30
        pred_weighted = weighted_avg + (drift * day_offset)
        
        # PRÉDICTION FINALE
        pred_final = (
            0.4 * pred_linear +
            0.3 * pred_trend +
            0.3 * pred_weighted
        )
        
        # INTERVALLE DE CONFIANCE
        volatility = np.std(prices_30)
        time_factor = 1 + (day_offset - 1) * 0.05
        confidence_interval = volatility * time_factor
        lower = pred_final - confidence_interval
        upper = pred_final + confidence_interval
        
        # NIVEAU DE CONFIANCE
        volatility_pct = (volatility / last_price) * 100
        
        if day_offset <= 5:
            confidence = "Élevée" if volatility_pct < 3 else "Moyenne" if volatility_pct < 6 else "Faible"
        elif day_offset <= 10:
            confidence = "Moyenne" if volatility_pct < 3 else "Faible"
        else:
            confidence = "Faible" if volatility_pct > 4 else "Moyenne"
        
        daily_predictions.append(pred_final)
        daily_lower_bounds.append(lower)
        daily_upper_bounds.append(upper)
        daily_confidence.append(confidence)
    
    avg_change = ((daily_predictions[-1] - last_price) / last_price) * 100
    
    return {
        'dates': future_business_days,
        'predictions': daily_predictions,
        'lower_bound': daily_lower_bounds,
        'upper_bound': daily_upper_bounds,
        'confidence_per_day': daily_confidence,
        'last_price': last_price,
        'avg_change_percent': avg_change,
        'overall_confidence': 'Moyenne' if abs(avg_change) < 5 else 'Faible'
    }

def save_predictions_to_db(conn, company_id, symbol, prediction_data):
    """Sauvegarde les 20 prédictions dans PostgreSQL"""
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM predictions WHERE company_id = %s", (company_id,))
            
            for i, pred_date in enumerate(prediction_data['dates']):
                cur.execute("""
                    INSERT INTO predictions (
                        company_id, prediction_date, predicted_price, 
                        lower_bound, upper_bound, confidence_level, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    company_id,
                    pred_date,
                    float(prediction_data['predictions'][i]),
                    float(prediction_data['lower_bound'][i]),
                    float(prediction_data['upper_bound'][i]),
                    prediction_data['confidence_per_day'][i]
                ))
            
            conn.commit()
            logging.info(f"   ✅ 20 prédictions sauvegardées pour {symbol}")
            return True
    
    except Exception as e:
        logging.error(f"❌ Erreur sauvegarde {symbol}: {e}")
        conn.rollback()
        return False

def process_company_prediction(conn, company_id, symbol):
    """Génère et sauvegarde les prédictions pour une société"""
    logging.info(f"--- Prédiction: {symbol} ---")
    
    try:
        query = """
            SELECT trade_date, price 
            FROM historical_data 
            WHERE company_id = %s 
            ORDER BY trade_date DESC 
            LIMIT 100
        """
        df = pd.read_sql(query, conn, params=(company_id,))
        
        if len(df) < 100:
            logging.warning(f"   ⚠️  Données insuffisantes ({len(df)} jours)")
            return False
        
        df = df.iloc[::-1].reset_index(drop=True)
        prediction_data = hybrid_prediction_daily(df['price'], df['trade_date'])
        
        if prediction_data is None:
            return False
        
        logging.info(f"   📊 Prix actuel: {prediction_data['last_price']:.2f} F CFA")
        logging.info(f"   📊 J+1: {prediction_data['predictions'][0]:.2f} F CFA ({prediction_data['confidence_per_day'][0]})")
        logging.info(f"   📊 J+10: {prediction_data['predictions'][9]:.2f} F CFA ({prediction_data['confidence_per_day'][9]})")
        logging.info(f"   📊 J+20: {prediction_data['predictions'][-1]:.2f} F CFA ({prediction_data['confidence_per_day'][-1]})")
        logging.info(f"   📊 Variation totale: {prediction_data['avg_change_percent']:.2f}%")
        
        save_predictions_to_db(conn, company_id, symbol, prediction_data)
        return True
    
    except Exception as e:
        logging.error(f"❌ Erreur {symbol}: {e}")
        return False

def run_prediction_analysis():
    logging.info("="*80)
    logging.info("🔮 ÉTAPE 3: PRÉDICTIONS (V9.0 - 20 PRÉDICTIONS/SOCIÉTÉ)")
    logging.info("="*80)
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies ORDER BY symbol;")
            companies = cur.fetchall()
        
        logging.info(f"📊 {len(companies)} société(s) à analyser")
        
        success_count = 0
        for company_id, symbol in companies:
            if process_company_prediction(conn, company_id, symbol):
                success_count += 1
        
        logging.info("\n" + "="*80)
        logging.info(f"✅ Prédictions terminées")
        logging.info(f"📊 Succès: {success_count}/{len(companies)} sociétés")
        logging.info(f"📊 Total prédictions: {success_count * 20} (20 par société)")
        logging.info("="*80)
    
    except Exception as e:
        logging.error(f"❌ Erreur: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_prediction_analysis()
