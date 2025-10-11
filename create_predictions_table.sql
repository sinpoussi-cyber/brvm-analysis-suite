-- ==============================================================================
-- TABLE: PREDICTIONS - Prédictions de prix (20 jours ouvrés)
-- ==============================================================================

-- Créer la table predictions
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    prediction_date DATE NOT NULL,
    predicted_price DECIMAL(10, 2) NOT NULL,
    lower_bound DECIMAL(10, 2),
    upper_bound DECIMAL(10, 2),
    confidence_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Une seule prédiction par société et par date
    CONSTRAINT unique_company_prediction_date UNIQUE(company_id, prediction_date)
);

-- Index pour recherche rapide
CREATE INDEX IF NOT EXISTS idx_predictions_company ON predictions(company_id);
CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(prediction_date);
CREATE INDEX IF NOT EXISTS idx_predictions_company_date ON predictions(company_id, prediction_date);

-- Trigger pour mise à jour automatique du timestamp
CREATE TRIGGER update_predictions_updated_at BEFORE UPDATE ON predictions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==============================================================================
-- VUE: Dernières prédictions par société
-- ==============================================================================
CREATE OR REPLACE VIEW v_latest_predictions AS
SELECT 
    c.symbol,
    c.name,
    p.prediction_date,
    p.predicted_price,
    p.lower_bound,
    p.upper_bound,
    p.confidence_level,
    p.created_at,
    (p.predicted_price - hd.price) as price_change,
    ((p.predicted_price - hd.price) / hd.price * 100) as price_change_percent
FROM predictions p
JOIN companies c ON p.company_id = c.id
LEFT JOIN LATERAL (
    SELECT price FROM historical_data
    WHERE company_id = c.id
    ORDER BY trade_date DESC
    LIMIT 1
) hd ON TRUE
ORDER BY c.symbol, p.prediction_date;

-- ==============================================================================
-- FONCTION: Nettoyer les anciennes prédictions (> 30 jours)
-- ==============================================================================
CREATE OR REPLACE FUNCTION clean_old_predictions()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM predictions
    WHERE prediction_date < CURRENT_DATE - INTERVAL '30 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Vérifier que la table a été créée
SELECT 'Table predictions créée avec succès !' as status;
