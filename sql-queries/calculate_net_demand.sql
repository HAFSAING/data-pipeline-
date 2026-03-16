-- =====================================================
-- CALCUL DE LA DEMANDE NETTE (MRP)
-- Formule: MAX(0, total_demand + safety_stock - (available_stock - reserved_stock))
-- Inclut: ranked_suppliers (1 fournisseur préféré par SKU) + estimated_cost
-- =====================================================

CREATE TABLE IF NOT EXISTS processed.net_demand AS

WITH demand AS (
  SELECT sku, total_demand
  FROM processed.aggregated_orders
),

inv AS (
  SELECT sku,
         SUM(available_stock) AS available_stock,
         SUM(reserved_stock)  AS reserved_stock
  FROM raw.inventory
  WHERE exec_date = '{{EXEC_DATE}}'
  GROUP BY sku
),

-- Sélectionner UN seul fournisseur préféré par SKU (priorité la plus basse = préféré)
ranked_suppliers AS (
  SELECT
    ps.sku,
    ps.supplier_id,
    ps.unit_cost,
    ps.priority,
    ROW_NUMBER() OVER (
      PARTITION BY ps.sku
      ORDER BY ps.priority ASC, ps.unit_cost ASC
    ) AS supplier_rank
  FROM postgresql.public.product_suppliers ps
),

master AS (
  SELECT p.sku, p.pack_size, p.moq, p.safety_stock,
         rs.supplier_id, rs.unit_cost
  FROM postgresql.public.products p
  INNER JOIN ranked_suppliers rs ON p.sku = rs.sku AND rs.supplier_rank = 1
),

net_calc AS (
  SELECT
    d.sku,
    m.supplier_id,
    d.total_demand,
    COALESCE(i.available_stock, 0)  AS available_stock,
    COALESCE(i.reserved_stock, 0)   AS reserved_stock,
    m.safety_stock,
    m.pack_size,
    m.moq,
    m.unit_cost,
    -- Formule MRP
    GREATEST(0,
      d.total_demand + m.safety_stock
      - (COALESCE(i.available_stock, 0) - COALESCE(i.reserved_stock, 0))
    ) AS raw_demand,
    -- Arrondi au pack_size supérieur
    CEIL(
      CAST(
        GREATEST(0,
          d.total_demand + m.safety_stock
          - (COALESCE(i.available_stock, 0) - COALESCE(i.reserved_stock, 0))
        ) AS DOUBLE
      ) / m.pack_size
    ) * m.pack_size AS final_order_qty
  FROM demand d
  LEFT JOIN inv     i ON d.sku = i.sku
  INNER JOIN master m ON d.sku = m.sku
)

SELECT
  sku,
  supplier_id,
  total_demand,
  available_stock,
  reserved_stock,
  safety_stock,
  pack_size,
  moq,
  unit_cost,
  raw_demand,
  final_order_qty,
  -- estimated_cost = quantité finale × coût unitaire
  CAST(final_order_qty AS DOUBLE) * unit_cost AS estimated_cost
FROM net_calc
WHERE final_order_qty >= moq