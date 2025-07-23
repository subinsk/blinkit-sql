CREATE TABLE IF NOT EXISTS all_blinkit_category_scraping_stream (
    created_at TEXT,
    l1_category_id INTEGER,
    l2_category_id INTEGER,
    store_id INTEGER,
    sku_id INTEGER,
    sku_name TEXT,
    selling_price REAL,
    mrp REAL,
    inventory INTEGER,
    image_url TEXT,
    brand_id INTEGER,
    brand TEXT,
    unit TEXT,
    PRIMARY KEY (created_at, sku_id, store_id)
);

CREATE TABLE IF NOT EXISTS blinkit_categories (
    l1_category TEXT,
    l1_category_id INTEGER,
    l2_category TEXT,
    l2_category_id INTEGER,
    PRIMARY KEY (l2_category_id)
);

CREATE TABLE IF NOT EXISTS blinkit_city_map (
    store_id INTEGER,
    city_name TEXT,
    PRIMARY KEY (store_id)
);
