-- ================================================
-- PROJECT: Stock Exchange Trading System
-- AUTHOR: Pedro Gabriel
-- DATABASE: PostgreSQL
-- DESCRIPTION: Complete database implementation for stock trading management
-- ================================================

-- ================================================
-- SECTION 1: DDL (Data Definition Language)
-- Description: Table structures, constraints, and relationships
-- ================================================

-- Table: investidor (Investor)
-- Stores all investors information (individuals and companies)
CREATE TABLE investidor (
    cpf_cnpj VARCHAR(14) PRIMARY KEY,          -- Brazilian tax ID (CPF=11, CNPJ=14)
    nome_completo VARCHAR(100) NOT NULL,        -- Full name or company name
    tipo_investidor VARCHAR(2) NOT NULL CHECK (tipo_investidor IN ('PF','PJ')), -- PF=Individual, PJ=Company
    email VARCHAR(100) NOT NULL,                -- Contact email
    telefone VARCHAR(20)                        -- Contact phone number
);

-- Table: acao (Stock)
-- Stores stock information for listed companies
CREATE TABLE acao (
    ticker VARCHAR(10) PRIMARY KEY,             -- Stock ticker symbol (e.g., PETR4, VALE3)
    nome_empresa VARCHAR(100) NOT NULL,         -- Company full name
    setor VARCHAR(50) NOT NULL,                 -- Industry sector (e.g., Energy, Mining)
    valor_mercado NUMERIC(15,2) NOT NULL CHECK (valor_mercado > 0) -- Market capitalization
);

-- Table: negociacao (Trade/Transaction)
-- Records all buy and sell operations
CREATE TABLE negociacao (
    id_negociacao SERIAL PRIMARY KEY,           -- Auto-incrementing transaction ID
    data_hora TIMESTAMP NOT NULL,               -- Transaction date and time
    tipo_operacao VARCHAR(10) NOT NULL CHECK (tipo_operacao IN ('COMPRA','VENDA')), -- BUY or SELL
    quantidade INTEGER NOT NULL CHECK (quantidade > 0), -- Number of shares traded
    valor_unitario NUMERIC(10,2) NOT NULL CHECK (valor_unitario > 0), -- Price per share at trade time
    cpf_cnpj VARCHAR(14) NOT NULL,              -- Investor FK
    ticker VARCHAR(10) NOT NULL,                -- Stock FK
    FOREIGN KEY (cpf_cnpj) REFERENCES investidor(cpf_cnpj),
    FOREIGN KEY (ticker) REFERENCES acao(ticker)
);

-- Table: historico_cotacao (Price History)
-- Stores historical stock prices for time series analysis
CREATE TABLE historico_cotacao (
    id_cotacao SERIAL PRIMARY KEY,               -- Auto-incrementing price record ID
    data_hora TIMESTAMP NOT NULL,                -- Price timestamp
    valor_cotacao NUMERIC(10,2) NOT NULL CHECK (valor_cotacao > 0), -- Stock price at that moment
    ticker VARCHAR(10) NOT NULL,                 -- Stock FK
    FOREIGN KEY (ticker) REFERENCES acao(ticker)
);

-- Table: saldo_carteira (Portfolio Balance)
-- Associative entity tracking current holdings per investor per stock
-- Composite primary key ensures one record per (investor, stock) pair
CREATE TABLE saldo_carteira (
    cpf_cnpj VARCHAR(14) NOT NULL,               -- Investor FK (part of composite PK)
    ticker VARCHAR(10) NOT NULL,                 -- Stock FK (part of composite PK)
    quantidade_acoes INTEGER NOT NULL CHECK (quantidade_acoes >= 0), -- Current shares held (never negative)
    PRIMARY KEY (cpf_cnpj, ticker),
    FOREIGN KEY (cpf_cnpj) REFERENCES investidor(cpf_cnpj),
    FOREIGN KEY (ticker) REFERENCES acao(ticker)
);

-- ================================================
-- SECTION 2: TRIGGERS (CORRECTED VERSION)
-- ================================================

-- Function: atualizar_saldo (Update Balance)
-- Automatically updates or creates portfolio balance when a trade is inserted
-- Validates sufficient shares before allowing a SELL operation
CREATE OR REPLACE FUNCTION atualizar_saldo()
RETURNS TRIGGER AS $$
DECLARE
    saldo_atual INTEGER;
BEGIN
    -- Validate sufficient balance for SELL operations
    IF NEW.tipo_operacao = 'VENDA' THEN
        SELECT COALESCE(quantidade_acoes, 0) INTO saldo_atual
        FROM saldo_carteira
        WHERE cpf_cnpj = NEW.cpf_cnpj AND ticker = NEW.ticker;
        
        -- Raise exception if trying to sell more shares than owned
        IF saldo_atual < NEW.quantidade THEN
            RAISE EXCEPTION 'Insufficient balance! Available: %, Attempted sale: %', 
                            saldo_atual, NEW.quantidade;
        END IF;
    END IF;
    
    -- Update or insert portfolio balance
    -- First, try to insert with positive quantity (always works)
    -- If record exists, update with proper sign (+ for BUY, - for SELL)
    INSERT INTO saldo_carteira (cpf_cnpj, ticker, quantidade_acoes)
    VALUES (NEW.cpf_cnpj, NEW.ticker, NEW.quantidade)  -- Always positive on INSERT
    ON CONFLICT (cpf_cnpj, ticker) 
    DO UPDATE SET quantidade_acoes = 
        saldo_carteira.quantidade_acoes + 
        (CASE WHEN NEW.tipo_operacao = 'COMPRA' THEN NEW.quantidade 
              ELSE -NEW.quantidade END);  -- Sign applied only in UPDATE
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: Executes after each new trade insertion
CREATE TRIGGER trigger_atualizar_saldo
AFTER INSERT ON negociacao
FOR EACH ROW
EXECUTE FUNCTION atualizar_saldo();

-- ================================================
-- SECTION 3: DML (Data Manipulation Language)
-- Description: Sample data for testing and demonstration
-- ================================================

-- Insert investors (individuals and companies)
INSERT INTO investidor (cpf_cnpj, nome_completo, tipo_investidor, email, telefone) VALUES 
('12345678900', 'Pedro Silva', 'PF', 'pedro.silva@email.com', '11988887777'),
('98765432000199', 'XP Investimentos', 'PJ', 'contato@xpinvest.com', '1133334444'),
('11122233344', 'Ana Costa', 'PF', 'ana.costa@email.com', '21977776666');

-- Insert stocks (companies listed on exchange)
INSERT INTO acao (ticker, nome_empresa, setor, valor_mercado) VALUES 
('PETR4', 'Petrobras', 'Energy', 500000000000.00),
('VALE3', 'Vale S.A.', 'Mining', 400000000000.00),
('ITUB4', 'Itaú Unibanco', 'Financial', 300000000000.00);

-- Insert price history for time series analysis
INSERT INTO historico_cotacao (data_hora, valor_cotacao, ticker) VALUES
-- PETR4 price history
('2025-03-20 10:00:00', 28.50, 'PETR4'),
('2025-03-20 11:00:00', 29.30, 'PETR4'),
('2025-03-20 12:00:00', 28.90, 'PETR4'),
('2025-03-21 10:00:00', 29.50, 'PETR4'),
-- VALE3 price history
('2025-03-20 10:30:00', 68.40, 'VALE3'),
('2025-03-20 11:30:00', 69.20, 'VALE3'),
('2025-03-21 10:00:00', 70.10, 'VALE3'),
-- ITUB4 price history
('2025-03-20 10:15:00', 32.50, 'ITUB4'),
('2025-03-21 10:00:00', 33.20, 'ITUB4');

-- Insert trades (transactions)
-- The trigger will automatically update portfolio balances
INSERT INTO negociacao (data_hora, tipo_operacao, quantidade, valor_unitario, cpf_cnpj, ticker) VALUES
-- Pedro buys 100 PETR4 shares
(NOW(), 'COMPRA', 100, 28.50, '12345678900', 'PETR4'),
-- Pedro buys 50 VALE3 shares  
(NOW(), 'COMPRA', 50, 68.40, '12345678900', 'VALE3'),
-- XP Investimentos buys 1000 ITUB4 shares
(NOW(), 'COMPRA', 1000, 32.50, '98765432000199', 'ITUB4'),
-- Pedro sells 30 PETR4 shares (validates balance automatically)
(NOW(), 'VENDA', 30, 29.30, '12345678900', 'PETR4'),
-- Ana buys 200 PETR4 shares
(NOW(), 'COMPRA', 200, 29.50, '11122233344', 'PETR4');

-- ================================================
-- SECTION 4: DQL (Data Query Language)
-- Description: Analytical queries for reporting and market analysis
-- ================================================

-- Query 1: Current portfolio balance for all investors
-- Shows what each investor currently owns
SELECT 
    i.nome_completo AS investor_name,
    i.tipo_investidor AS investor_type,
    s.ticker,
    a.nome_empresa AS company_name,
    s.quantidade_acoes AS shares_held,
    a.valor_mercado AS market_cap
FROM saldo_carteira s
JOIN investidor i ON s.cpf_cnpj = i.cpf_cnpj
JOIN acao a ON s.ticker = a.ticker
WHERE s.quantidade_acoes > 0
ORDER BY i.nome_completo, s.ticker;

-- Query 2: Complete transaction history with details
-- Shows all trades with investor and stock information
SELECT 
    n.id_negociacao AS trade_id,
    n.data_hora AS trade_date,
    i.nome_completo AS investor,
    n.tipo_operacao AS operation,
    n.quantidade AS shares,
    n.valor_unitario AS unit_price,
    (n.quantidade * n.valor_unitario) AS total_value,
    a.nome_empresa AS company
FROM negociacao n
JOIN investidor i ON n.cpf_cnpj = i.cpf_cnpj
JOIN acao a ON n.ticker = a.ticker
ORDER BY n.data_hora DESC;

-- Query 3: Current portfolio value using latest stock prices
-- Calculates real-time portfolio value based on most recent quotes
SELECT 
    i.nome_completo AS investor,
    s.ticker,
    s.quantidade_acoes AS shares,
    hc.valor_cotacao AS current_price,
    (s.quantidade_acoes * hc.valor_cotacao) AS position_value
FROM saldo_carteira s
JOIN investidor i ON s.cpf_cnpj = i.cpf_cnpj
JOIN (
    -- Subquery to get latest price for each stock
    SELECT DISTINCT ON (ticker) ticker, valor_cotacao, data_hora
    FROM historico_cotacao
    ORDER BY ticker, data_hora DESC
) hc ON hc.ticker = s.ticker
WHERE s.quantidade_acoes > 0
ORDER BY position_value DESC;

-- Query 4: Price history for time series analysis
-- Shows stock price evolution over time for a specific stock
SELECT 
    data_hora AS timestamp,
    valor_cotacao AS price,
    ticker
FROM historico_cotacao
WHERE ticker = 'PETR4'
ORDER BY data_hora DESC;

-- Query 5: Average purchase price per investor per stock
-- Helps calculate profit/loss by comparing purchase average with current price
SELECT 
    i.nome_completo AS investor,
    n.ticker,
    AVG(n.valor_unitario) AS avg_purchase_price,
    SUM(n.quantidade) AS total_shares_purchased,
    COUNT(*) AS number_of_trades
FROM negociacao n
JOIN investidor i ON n.cpf_cnpj = i.cpf_cnpj
WHERE n.tipo_operacao = 'COMPRA'
GROUP BY i.nome_completo, n.ticker
ORDER BY i.nome_completo, n.ticker;

-- Query 6: Daily trading volume analysis
-- Shows which stocks are most actively traded
SELECT 
    DATE(n.data_hora) AS trade_date,
    a.nome_empresa AS company,
    COUNT(*) AS number_of_trades,
    SUM(n.quantidade) AS total_shares_traded,
    SUM(n.quantidade * n.valor_unitario) AS total_volume
FROM negociacao n
JOIN acao a ON n.ticker = a.ticker
GROUP BY DATE(n.data_hora), a.nome_empresa
ORDER BY trade_date DESC, total_volume DESC;

-- Query 7: Investor portfolio diversification
-- Shows how many different stocks each investor holds
SELECT 
    i.nome_completo AS investor,
    COUNT(DISTINCT s.ticker) AS distinct_stocks,
    SUM(s.quantidade_acoes) AS total_shares,
    CASE 
        WHEN COUNT(DISTINCT s.ticker) >= 3 THEN 'Diversified'
        WHEN COUNT(DISTINCT s.ticker) >= 1 THEN 'Concentrated'
        ELSE 'No holdings'
    END AS portfolio_profile
FROM investidor i
LEFT JOIN saldo_carteira s ON i.cpf_cnpj = s.cpf_cnpj AND s.quantidade_acoes > 0
GROUP BY i.nome_completo, i.cpf_cnpj
ORDER BY distinct_stocks DESC;

-- ================================================
-- END OF SCRIPT
-- ================================================