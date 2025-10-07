-- ==============================================================================
-- SCRIPT D'INITIALISATION DE LA BASE DE DONNÉES BRVM
-- ==============================================================================
-- À exécuter sur Supabase ou toute base PostgreSQL
-- ==============================================================================

-- 1. TABLE COMPANIES (Sociétés cotées)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour recherche rapide par symbole
CREATE INDEX IF NOT EXISTS idx_companies_symbol ON companies(symbol);

-- Insertion des sociétés cotées à la BRVM
INSERT INTO companies (symbol, name, sector) VALUES
-- Secteur Bancaire
('SGBC', 'SOCIETE GENERALE CI', 'Banque'),
('BICC', 'BICI CI', 'Banque'),
('NSBC', 'NSIA BANQUE CI', 'Banque'),
('ECOC', 'ECOBANK COTE D''IVOIRE', 'Banque'),
('BOAC', 'BANK OF AFRICA CI', 'Banque'),
('SIBC', 'SOCIETE IVOIRIENNE DE BANQUE', 'Banque'),
('BOABF', 'BANK OF AFRICA BF', 'Banque'),
('BOAS', 'BANK OF AFRICA SN', 'Banque'),
('BOAM', 'BANK OF AFRICA ML', 'Banque'),
('BOAN', 'BANK OF AFRICA NG', 'Banque'),
('BOAB', 'BANK OF AFRICA BN', 'Banque'),
('BICB', 'BICI BN', 'Banque'),
('CBIBF', 'CORIS BANKING INTERNATIONAL', 'Banque'),
('ETIT', 'ECOBANK TRANSNATIONAL INCORPORATED', 'Banque'),
('ORGT', 'ORAGROUP TOGO', 'Banque'),
('SAFC', 'SAFCA CI', 'Banque'),
('SOGC', 'SOGB CI', 'Banque'),

-- Télécommunications
('SNTS', 'SONATEL SN', 'Télécommunications'),
('ORAC', 'ORANGE COTE D''IVOIRE', 'Télécommunications'),
('ONTBF', 'ONATEL BF', 'Télécommunications'),

-- Industrie Agroalimentaire
('PALC', 'PALM CI', 'Industrie'),
('NTLC', 'NESTLE CI', 'Industrie'),
('UNLC', 'UNILEVER CI', 'Industrie'),
('SLBC', 'SOLIBRA CI', 'Industrie'),
('SICC', 'SICOR CI', 'Industrie'),
('SPHC', 'SAPH CI', 'Industrie'),
('SCRC', 'SUCRIVOIRE', 'Industrie'),
('STBC', 'SITAB CI', 'Industrie'),

-- Énergie et Distribution
('TTLC', 'TOTALENERGIES MARKETING CI', 'Énergie'),
('TTLS', 'TOTALENERGIES MARKETING SN', 'Énergie'),
('SHEC', 'VIVO ENERGY CI', 'Énergie'),
('CIEC', 'CIE CI', 'Énergie'),

-- Distribution et Services
('CFAC', 'CFAO MOTORS CI', 'Distribution'),
('PRSC', 'TRACTAFRIC MOTORS CI', 'Distribution'),
('SDSC', 'AFRICA GLOBAL LOGISTICS', 'Distribution'),
('ABJC', 'SERVAIR ABIDJAN CI', 'Services'),
('BNBC', 'BERNABE CI', 'Distribution'),
('NEIC', 'NEI-CEDA CI', 'Distribution'),
('UNXC', 'UNIWAX CI', 'Industrie'),
('LNBB', 'LOTERIE NATIONALE BN', 'Services'),

-- Industrie et Autres
('CABC', 'SICABLE CI', 'Industrie'),
('FTSC', 'FILTISAC CI', 'Industrie'),
('SDCC', 'SODE CI', 'Distribution'),
('SEMC', 'EVIOSYS PACKAGING', 'Industrie'),
('SIVC', 'AIR LIQUIDE CI', 'Industrie'),
('STAC', 'SETAO CI', 'Industrie'),
('SMBC', 'SMB CI', 'Industrie')

ON CONFLICT (symbol) DO NOTHING;

-- 2. TABLE HISTORICAL_DATA (Données de marché quotidiennes)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS historical_data (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    price DECIMAL(10, 2),
    volume INTEGER DEFAULT 0,
    value DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_company_date UNIQUE(company_id, trade_date)
);

-- Index pour recherche rapide
CREATE INDEX IF NOT EXISTS idx_historical_data_date ON historical_data(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_historical_data_company ON historical_data(company_id);
CREATE INDEX IF NOT EXISTS idx_historical_data_company_date ON historical_data(company_id, trade_date DESC);

-- 3. TABLE TECHNICAL_ANALYSIS (Analyses techniques)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS technical_analysis (
    id SERIAL PRIMARY KEY,
    historical_data_id INTEGER UNIQUE NOT NULL REFERENCES historical_data(id) ON DELETE CASCADE,
    
    -- Moyennes Mobiles
    mm5 DECIMAL(10, 2),
    mm10 DECIMAL(10, 2),
    mm20 DECIMAL(10, 2),
    mm50 DECIMAL(10, 2),
    mm_decision VARCHAR(50),
    
    -- Bandes de Bollinger
    bollinger_central DECIMAL(10, 2),
    bollinger_inferior DECIMAL(10, 2),
    bollinger_superior DECIMAL(10, 2),
    bollinger_decision VARCHAR(50),
    
    -- MACD
    macd_line DECIMAL(10, 4),
    signal_line DECIMAL(10, 4),
    histogram DECIMAL(10, 4),
    macd_decision VARCHAR(50),
    
    -- RSI
    rsi DECIMAL(5, 2),
    rsi_decision VARCHAR(50),
    
    -- Stochastique
    stochastic_k DECIMAL(5, 2),
    stochastic_d DECIMAL(5, 2),
    stochastic_decision VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour jointures rapides
CREATE INDEX IF NOT EXISTS idx_technical_analysis_data ON technical_analysis(historical_data_id);

-- 4. TABLE FUNDAMENTAL_ANALYSIS (Analyses fondamentales IA)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS fundamental_analysis (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    report_url VARCHAR(500) UNIQUE NOT NULL,
    report_title VARCHAR(500),
    report_date DATE,
    analysis_summary TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour recherche rapide
CREATE INDEX IF NOT EXISTS idx_fundamental_company ON fundamental_analysis(company_id);
CREATE INDEX IF NOT EXISTS idx_fundamental_date ON fundamental_analysis(report_date DESC);

-- ==============================================================================
-- VUES UTILES POUR L'API
-- ==============================================================================

-- Vue : Dernières données de marché avec analyses
CREATE OR REPLACE VIEW v_latest_market_data AS
SELECT 
    c.symbol,
    c.name,
    c.sector,
    hd.trade_date,
    hd.price,
    hd.volume,
    hd.value,
    ta.mm_decision,
    ta.bollinger_decision,
    ta.macd_decision,
    ta.rsi_decision,
    ta.stochastic_decision
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM historical_data
    WHERE company_id = c.id
    ORDER BY trade_date DESC
    LIMIT 1
) hd ON TRUE
LEFT JOIN technical_analysis ta ON hd.id = ta.historical_data_id
ORDER BY c.symbol;

-- Vue : Statistiques par société
CREATE OR REPLACE VIEW v_company_statistics AS
SELECT 
    c.symbol,
    c.name,
    COUNT(DISTINCT hd.id) as total_days,
    MIN(hd.trade_date) as first_date,
    MAX(hd.trade_date) as last_date,
    COUNT(DISTINCT fa.id) as total_reports
FROM companies c
LEFT JOIN historical_data hd ON c.id = hd.company_id
LEFT JOIN fundamental_analysis fa ON c.id = fa.company_id
GROUP BY c.symbol, c.name
ORDER BY c.symbol;

-- ==============================================================================
-- FONCTION : Mise à jour automatique du timestamp
-- ==============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers pour mise à jour automatique
CREATE TRIGGER update_companies_updated_at BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_technical_analysis_updated_at BEFORE UPDATE ON technical_analysis
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fundamental_analysis_updated_at BEFORE UPDATE ON fundamental_analysis
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==============================================================================
-- FIN DU SCRIPT
-- ==============================================================================
-- Base de données initialisée avec succès !
-- Vous pouvez maintenant exécuter le workflow GitHub Actions.
-- ==============================================================================
