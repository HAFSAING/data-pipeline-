-- =====================================================
-- INITIALISATION BASE POSTGRESQL - PROCUREMENT PIPELINE
-- =====================================================

-- Table des produits
CREATE TABLE IF NOT EXISTS products (
    sku VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    supplier_id VARCHAR(20) NOT NULL,
    pack_size INT DEFAULT 1,
    moq INT DEFAULT 1,
    lead_time_days INT DEFAULT 1,
    safety_stock INT DEFAULT 0
);

-- Table des fournisseurs
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    contact_email VARCHAR(100),
    lead_time_days INT DEFAULT 7,
    reliability_score DECIMAL(3,2) DEFAULT 0.95
);

-- =====================================================
-- TABLE product_suppliers (relation produit <-> fournisseur)
-- Permet plusieurs fournisseurs par SKU avec priorité et coût
-- =====================================================
CREATE TABLE IF NOT EXISTS product_suppliers (
    sku VARCHAR(20) NOT NULL REFERENCES products(sku),
    supplier_id VARCHAR(20) NOT NULL REFERENCES suppliers(supplier_id),
    unit_cost DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    priority INT DEFAULT 1,           -- 1 = fournisseur préféré
    is_preferred BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (sku, supplier_id)
);

CREATE INDEX IF NOT EXISTS idx_ps_sku      ON product_suppliers(sku);
CREATE INDEX IF NOT EXISTS idx_ps_supplier ON product_suppliers(supplier_id);
CREATE INDEX IF NOT EXISTS idx_ps_priority ON product_suppliers(priority);

-- Table des commandes agrégées
CREATE TABLE IF NOT EXISTS aggregated_orders (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(20) NOT NULL,
    total_demand INT NOT NULL,
    exec_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de la demande nette (avec estimated_cost ajouté)
CREATE TABLE IF NOT EXISTS net_demand (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(20) NOT NULL,
    supplier_id VARCHAR(20) NOT NULL,
    total_demand INT DEFAULT 0,
    available_stock INT DEFAULT 0,
    safety_stock INT DEFAULT 0,
    net_demand INT DEFAULT 0,
    moq INT DEFAULT 1,
    final_order_qty INT DEFAULT 0,
    unit_cost DECIMAL(10,2) DEFAULT 0.00,
    estimated_cost DECIMAL(12,2) DEFAULT 0.00,   -- NOUVEAU: coût total estimé
    exec_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DONNÉES INITIALES
-- =====================================================

-- Produits
INSERT INTO products (sku, name, supplier_id, pack_size, moq, lead_time_days, safety_stock) VALUES
    ('SKU_1001', 'Bananas',  'SUP_001', 12, 24, 2, 50),
    ('SKU_1002', 'Milk 1L',  'SUP_002',  6, 18, 1, 30),
    ('SKU_1003', 'Yogurt',   'SUP_002',  8, 16, 2, 20)
ON CONFLICT (sku) DO NOTHING;

-- Fournisseurs
INSERT INTO suppliers (supplier_id, name, contact_email, lead_time_days, reliability_score) VALUES
    ('SUP_001', 'FreshFruit Co', 'orders@freshfruit.com', 2, 0.98),
    ('SUP_002', 'DairyMax Ltd',  'procure@dairymax.com',  1, 0.95),
    ('SUP_003', 'BioFarm Ltd',   'bio@biofarm.com',       3, 0.90)
ON CONFLICT (supplier_id) DO NOTHING;

-- Relations produit <-> fournisseur avec coûts et priorités
INSERT INTO product_suppliers (sku, supplier_id, unit_cost, priority, is_preferred) VALUES
    -- Bananas : SUP_001 préféré, SUP_003 alternatif
    ('SKU_1001', 'SUP_001', 1.20, 1, TRUE),
    ('SKU_1001', 'SUP_003', 1.45, 2, FALSE),
    -- Milk 1L : SUP_002 préféré
    ('SKU_1002', 'SUP_002', 0.85, 1, TRUE),
    -- Yogurt : SUP_002 préféré
    ('SKU_1003', 'SUP_002', 1.10, 1, TRUE)
ON CONFLICT (sku, supplier_id) DO NOTHING;

-- Confirmation
SELECT 'PostgreSQL initialized successfully' AS status;