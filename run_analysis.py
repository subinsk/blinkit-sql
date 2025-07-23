import sqlite3
import pandas as pd
import os

def setup_database():
    conn = sqlite3.connect('blinkit_analysis.db')
    
    with open('setup_database.sql', 'r') as f:
        setup_sql = f.read()
    
    conn.executescript(setup_sql)
    
    print("Loading all_blinkit_category_scraping_stream...")
    stream_df = pd.read_csv('inputs/all_blinkit_category_scraping_stream.csv')
    stream_df.to_sql('all_blinkit_category_scraping_stream', conn, if_exists='replace', index=False)
    
    print("Loading blinkit_categories...")
    categories_df = pd.read_csv('inputs/blinkit_categories.csv')
    categories_df.to_sql('blinkit_categories', conn, if_exists='replace', index=False)
    
    print("Loading blinkit_city_map...")
    city_map_df = pd.read_csv('inputs/blinkit_city_map.csv')
    city_map_df.to_sql('blinkit_city_map', conn, if_exists='replace', index=False)
    
    print("Database setup complete!")
    return conn

def execute_city_insights_query(conn):
    print("Creating optimized city insights table...")
    
    conn.execute("DROP TABLE IF EXISTS blinkit_city_insights")
    
    # Create the target table structure
    create_table_sql = """
    CREATE TABLE blinkit_city_insights (
        date TEXT,
        brand_id INTEGER,
        brand TEXT,
        image_url TEXT,
        city_name TEXT,
        sku_id INTEGER,
        sku_name TEXT,
        category_id INTEGER,
        category_name TEXT,
        sub_category_id INTEGER,
        sub_category_name TEXT,
        est_qty_sold REAL,
        est_sales_sp REAL,
        est_sales_mrp REAL,
        listed_ds_count INTEGER,
        ds_count INTEGER,
        wt_osa REAL,
        wt_osa_ls REAL,
        mrp REAL,
        sp REAL,
        discount REAL
    )
    """
    conn.execute(create_table_sql)
    
    # Get unique cities to process in batches
    cities = pd.read_sql_query("SELECT DISTINCT city_name FROM blinkit_city_map", conn)['city_name'].tolist()
    print(f"Processing {len(cities)} cities...")
    
    total_rows = 0
    batch_size = 5  # Process 5 cities at a time
    
    for i in range(0, len(cities), batch_size):
        city_batch = cities[i:i+batch_size]
        city_list = "'" + "','".join(city_batch) + "'"
        print(f"Processing cities: {city_batch}")
        
        # Simplified batch query
        batch_query = f"""
        INSERT INTO blinkit_city_insights
        SELECT 
            DATE(s.created_at) as date,
            s.brand_id,
            s.brand,
            s.image_url,
            c.city_name,
            s.sku_id,
            s.sku_name,
            bc.l1_category_id as category_id,
            bc.l1_category as category_name,
            bc.l2_category_id as sub_category_id,
            bc.l2_category as sub_category_name,
            CASE 
                WHEN AVG(s.inventory) > 0 THEN ROUND(AVG(s.inventory) * 0.1, 2)
                ELSE 0.0
            END as est_qty_sold,
            ROUND(AVG(s.inventory) * 0.1 * AVG(s.selling_price), 2) as est_sales_sp,
            ROUND(AVG(s.inventory) * 0.1 * AVG(s.mrp), 2) as est_sales_mrp,
            COUNT(DISTINCT s.store_id) as listed_ds_count,
            (SELECT COUNT(DISTINCT store_id) FROM all_blinkit_category_scraping_stream) as ds_count,
            ROUND(CAST(COUNT(DISTINCT CASE WHEN s.inventory > 0 THEN s.store_id END) AS FLOAT) / 
                  (SELECT COUNT(DISTINCT store_id) FROM all_blinkit_category_scraping_stream), 4) as wt_osa,
            ROUND(CAST(COUNT(DISTINCT CASE WHEN s.inventory > 0 THEN s.store_id END) AS FLOAT) / 
                  COUNT(DISTINCT s.store_id), 4) as wt_osa_ls,
            AVG(s.mrp) as mrp,
            AVG(s.selling_price) as sp,
            ROUND((AVG(s.mrp) - AVG(s.selling_price)) / AVG(s.mrp), 4) as discount
        FROM all_blinkit_category_scraping_stream s
        INNER JOIN blinkit_city_map c ON s.store_id = c.store_id
        LEFT JOIN blinkit_categories bc ON s.l2_category_id = bc.l2_category_id
        WHERE c.city_name IN ({city_list})
        AND bc.l1_category_id IS NOT NULL
        GROUP BY DATE(s.created_at), s.sku_id, c.city_name
        """
        
        conn.execute(batch_query)
        conn.commit()
        
        batch_count = conn.execute("SELECT COUNT(*) FROM blinkit_city_insights WHERE city_name IN (" + city_list + ")").fetchone()[0]
        total_rows += batch_count
        print(f"Batch completed. Rows added: {batch_count}")
    
    # Export results
    result_df = pd.read_sql_query("SELECT * FROM blinkit_city_insights ORDER BY date, city_name, sku_id", conn)
    
    output_file = 'blinkit_city_insights_output.csv'
    result_df.to_csv(output_file, index=False)
    
    print(f"Query executed successfully!")
    print(f"Output saved to: {output_file}")
    print(f"Total rows: {len(result_df)}")
    
    return result_df

if __name__ == "__main__":
    conn = setup_database()
    result = execute_city_insights_query(conn)
    conn.close()
    print("Process completed!")
