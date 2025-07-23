-- Create blinkit_city_insights derived table
-- This query calculates estimated quantity sold based on inventory movement over time

CREATE TABLE blinkit_city_insights AS
WITH inventory_changes AS (
    SELECT 
        s.created_at,
        s.sku_id,
        s.sku_name,
        s.store_id,
        s.inventory,
        s.selling_price,
        s.mrp,
        s.brand_id,
        s.brand,
        s.image_url,
        s.l1_category_id,
        s.l2_category_id,
        c.city_name,
        LAG(s.inventory) OVER (
            PARTITION BY s.sku_id, s.store_id 
            ORDER BY s.created_at
        ) as prev_inventory,
        LAG(s.created_at) OVER (
            PARTITION BY s.sku_id, s.store_id 
            ORDER BY s.created_at
        ) as prev_created_at
    FROM all_blinkit_category_scraping_stream s
    INNER JOIN blinkit_city_map c ON s.store_id = c.store_id
),
qty_sold_calculation AS (
    SELECT 
        *,
        CASE 
            WHEN prev_inventory IS NULL THEN 0
            WHEN inventory < prev_inventory THEN prev_inventory - inventory
            WHEN inventory >= prev_inventory THEN (
                SELECT COALESCE(AVG(CASE 
                    WHEN prev_inventory IS NOT NULL AND inventory < prev_inventory 
                    THEN prev_inventory - inventory 
                    ELSE 0 
                END), 0)
                FROM inventory_changes ic2
                WHERE ic2.sku_id = inventory_changes.sku_id 
                AND ic2.store_id = inventory_changes.store_id
                AND ic2.created_at < inventory_changes.created_at
                LIMIT 5
            )
            ELSE 0
        END as est_qty_sold_per_store
    FROM inventory_changes
),
city_aggregation AS (
    SELECT 
        DATE(created_at) as date,
        sku_id,
        sku_name,
        brand_id,
        brand,
        image_url,
        city_name,
        l1_category_id,
        l2_category_id,
        SUM(est_qty_sold_per_store) as est_qty_sold,
        AVG(selling_price) as avg_selling_price,
        AVG(mrp) as avg_mrp
    FROM qty_sold_calculation
    GROUP BY DATE(created_at), sku_id, city_name
),
store_counts AS (
    SELECT 
        sku_id,
        COUNT(DISTINCT store_id) as listed_ds_count,
        (SELECT COUNT(DISTINCT store_id) FROM all_blinkit_category_scraping_stream) as ds_count
    FROM all_blinkit_category_scraping_stream
    GROUP BY sku_id
),
stock_availability AS (
    SELECT 
        sku_id,
        COUNT(DISTINCT CASE WHEN inventory > 0 THEN store_id END) as in_stock_stores,
        COUNT(DISTINCT store_id) as total_listed_stores
    FROM all_blinkit_category_scraping_stream
    GROUP BY sku_id
)
SELECT 
    ca.date,
    ca.brand_id,
    ca.brand,
    ca.image_url,
    ca.city_name,
    ca.sku_id,
    ca.sku_name,
    bc.l1_category_id as category_id,
    bc.l1_category as category_name,
    bc.l2_category_id as sub_category_id,
    bc.l2_category as sub_category_name,
    ROUND(ca.est_qty_sold, 2) as est_qty_sold,
    ROUND(ca.est_qty_sold * ca.avg_selling_price, 2) as est_sales_sp,
    ROUND(ca.est_qty_sold * ca.avg_mrp, 2) as est_sales_mrp,
    sc.listed_ds_count,
    sc.ds_count,
    ROUND(CAST(sa.in_stock_stores AS FLOAT) / sc.ds_count, 4) as wt_osa,
    ROUND(CAST(sa.in_stock_stores AS FLOAT) / sc.listed_ds_count, 4) as wt_osa_ls,
    ca.avg_mrp as mrp,
    ca.avg_selling_price as sp,
    ROUND((ca.avg_mrp - ca.avg_selling_price) / ca.avg_mrp, 4) as discount
FROM city_aggregation ca
LEFT JOIN blinkit_categories bc ON ca.l2_category_id = bc.l2_category_id
LEFT JOIN store_counts sc ON ca.sku_id = sc.sku_id
LEFT JOIN stock_availability sa ON ca.sku_id = sa.sku_id
WHERE bc.l1_category_id IS NOT NULL
ORDER BY ca.date, ca.city_name, ca.sku_id;
