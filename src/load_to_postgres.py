import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

print("=== COMPLETE POSTGRESQL DATA LOADER ===")

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',
    'user': 'postgres',
    'password': 'Vikas@6084'
}

def generate_sample_data():
    """Generate all sample CSV files"""
    print("1. 📦 GENERATING SAMPLE DATA...")
    
    # Create data folder if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')
        print("   Created data/ folder")
    
    # Generate customers
    customers_data = []
    for i in range(1, 101):
        customers_data.append({
            'customer_id': i,
            'name': f'Customer_{i}',
            'email': f'customer{i}@example.com',
            'country': random.choice(['USA', 'UK', 'Canada', 'Australia'])
        })
    
    # Generate products
    products_data = []
    for i in range(1, 21):
        products_data.append({
            'product_id': i,
            'name': f'Product_{i}',
            'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home']),
            'price': round(random.uniform(10, 100), 2)
        })
    
    # Generate orders
    orders_data = []
    for i in range(1, 201):
        order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 180))
        orders_data.append({
            'order_id': i,
            'customer_id': random.randint(1, 100),
            'product_id': random.randint(1, 20),
            'order_date': order_date.strftime('%Y-%m-%d'),
            'amount': round(random.uniform(20, 500), 2),
            'status': random.choice(['completed', 'cancelled', 'pending'])
        })
    
    # Save raw data
    pd.DataFrame(customers_data).to_csv('data/customers.csv', index=False)
    pd.DataFrame(products_data).to_csv('data/products.csv', index=False)
    pd.DataFrame(orders_data).to_csv('data/orders.csv', index=False)
    print("   ✅ Raw data files created")
    
    return True

def create_analytics_files():
    """Create the 5 required analytics CSV files"""
    print("2. 🧹 CLEANING AND CREATING ANALYTICS FILES...")
    
    # Load raw data
    customers = pd.read_csv('data/customers.csv')
    products = pd.read_csv('data/products.csv')
    orders = pd.read_csv('data/orders.csv')
    
    # Clean data
    orders_clean = orders.dropna()
    orders_clean['order_date'] = pd.to_datetime(orders_clean['order_date'])
    
    # Join data to create order_details
    merged = orders_clean.merge(customers, on='customer_id', how='left')
    order_details = merged.merge(products, on='product_id', how='left', suffixes=('_order', '_product'))
    
    # Create final order_details table
    order_details = order_details[[
        'order_id', 'name_order', 'country', 'name_product', 
        'category', 'amount', 'status', 'order_date'
    ]]
    order_details.columns = [
        'order_id', 'customer_name', 'country', 'product_name',
        'category', 'amount', 'status', 'order_date'
    ]
    
    # Save order_details
    order_details.to_csv('data/order_details.csv', index=False)
    print("   ✅ order_details.csv created")
    
    # Create top_customers.csv
    top_customers = order_details[order_details['status'] == 'completed']
    top_customers = top_customers.groupby(['customer_name', 'country'])['amount'].sum().reset_index()
    top_customers.columns = ['customer_name', 'country', 'total_spend']
    top_customers = top_customers.sort_values('total_spend', ascending=False).head(5)
    top_customers.to_csv('data/top_customers.csv', index=False)
    print("   ✅ top_customers.csv created")
    
    # Create monthly_revenue.csv
    monthly_revenue = order_details[order_details['status'] == 'completed']
    monthly_revenue['year'] = pd.to_datetime(monthly_revenue['order_date']).dt.year
    monthly_revenue['month'] = pd.to_datetime(monthly_revenue['order_date']).dt.month
    monthly_revenue = monthly_revenue.groupby(['year', 'month'])['amount'].sum().reset_index()
    monthly_revenue.columns = ['year', 'month', 'monthly_revenue']
    monthly_revenue.to_csv('data/monthly_revenue.csv', index=False)
    print("   ✅ monthly_revenue.csv created")
    
    # Create popular_categories.csv
    popular_categories = order_details[order_details['status'] == 'completed']
    popular_categories = popular_categories.groupby('category').size().reset_index()
    popular_categories.columns = ['category', 'order_count']
    popular_categories = popular_categories.sort_values('order_count', ascending=False)
    popular_categories.to_csv('data/popular_categories.csv', index=False)
    print("   ✅ popular_categories.csv created")
    
    # Create order_status.csv
    order_status = order_details.groupby('status').size().reset_index()
    order_status.columns = ['status', 'order_count']
    order_status['percentage'] = (order_status['order_count'] / order_status['order_count'].sum() * 100).round(2)
    order_status.to_csv('data/order_status.csv', index=False)
    print("   ✅ order_status.csv created")
    
    return True

def setup_postgres_tables():
    """Create tables in PostgreSQL if they don't exist"""
    print("3. 🗄️ SETTING UP POSTGRESQL TABLES...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS order_details (
                order_id INTEGER,
                customer_name VARCHAR(255),
                country VARCHAR(100),
                product_name VARCHAR(255),
                category VARCHAR(100),
                amount DECIMAL(10,2),
                status VARCHAR(50),
                order_date DATE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS top_customers (
                customer_name VARCHAR(255),
                country VARCHAR(100),
                total_spend DECIMAL(10,2)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS monthly_revenue (
                year INTEGER,
                month INTEGER,
                monthly_revenue DECIMAL(10,2)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS popular_categories (
                category VARCHAR(100),
                order_count INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS order_status (
                status VARCHAR(50),
                order_count INTEGER,
                percentage DECIMAL(5,2)
            )
            """
        ]
        
        for i, sql in enumerate(tables_sql):
            cursor.execute(sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("   ✅ PostgreSQL tables created/verified")
        return True
        
    except Exception as e:
        print(f"   ❌ Error setting up PostgreSQL tables: {e}")
        return False

def load_data_to_postgres():
    """Load all CSV data into PostgreSQL"""
    print("4. 📤 LOADING DATA INTO POSTGRESQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Load order_details
        print("   Loading order_details...")
        order_details = pd.read_csv("data/order_details.csv")
        for _, row in order_details.iterrows():
            cursor.execute("""
                INSERT INTO order_details 
                (order_id, customer_name, country, product_name, category, amount, status, order_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        print("   ✅ order_details loaded")
        
        # Load top_customers
        print("   Loading top_customers...")
        top_customers = pd.read_csv("data/top_customers.csv")
        for _, row in top_customers.iterrows():
            cursor.execute("""
                INSERT INTO top_customers (customer_name, country, total_spend)
                VALUES (%s, %s, %s)
            """, tuple(row))
        print("   ✅ top_customers loaded")
        
        # Load monthly_revenue
        print("   Loading monthly_revenue...")
        monthly_revenue = pd.read_csv("data/monthly_revenue.csv")
        for _, row in monthly_revenue.iterrows():
            cursor.execute("""
                INSERT INTO monthly_revenue (year, month, monthly_revenue)
                VALUES (%s, %s, %s)
            """, tuple(row))
        print("   ✅ monthly_revenue loaded")
        
        # Load popular_categories
        print("   Loading popular_categories...")
        popular_categories = pd.read_csv("data/popular_categories.csv")
        for _, row in popular_categories.iterrows():
            cursor.execute("""
                INSERT INTO popular_categories (category, order_count)
                VALUES (%s, %s)
            """, tuple(row))
        print("   ✅ popular_categories loaded")
        
        # Load order_status
        print("   Loading order_status...")
        order_status = pd.read_csv("data/order_status.csv")
        for _, row in order_status.iterrows():
            cursor.execute("""
                INSERT INTO order_status (status, order_count, percentage)
                VALUES (%s, %s, %s)
            """, tuple(row))
        print("   ✅ order_status loaded")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n🎉 ALL DATA SUCCESSFULLY LOADED INTO POSTGRESQL!")
        return True
        
    except Exception as e:
        print(f"❌ Error loading data into PostgreSQL: {e}")
        return False

def verify_postgres_data():
    """Verify data was loaded into PostgreSQL"""
    print("5. ✅ VERIFYING POSTGRESQL DATA...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Check record counts in each table
        tables = ['order_details', 'top_customers', 'monthly_revenue', 'popular_categories', 'order_status']
        
        for table in tables:
            df = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", conn)
            count = df['count'].iloc[0]
            print(f"   {table}: {count} records")
        
        conn.close()
        print("   ✅ Data verification completed!")
        return True
        
    except Exception as e:
        print(f"   ❌ Verification failed: {e}")
        return False

def main():
    """Main function to run the complete pipeline"""
    try:
        # Step 1: Generate sample data
        if not generate_sample_data():
            return False
        
        # Step 2: Create analytics files
        if not create_analytics_files():
            return False
        
        # Step 3: Setup PostgreSQL tables
        if not setup_postgres_tables():
            return False
        
        # Step 4: Load data into PostgreSQL
        if not load_data_to_postgres():
            return False
        
        # Step 5: Verify data
        if not verify_postgres_data():
            return False
        
        print("\n" + "="*50)
        print("🎉 COMPLETE SUCCESS! All 5 CSV tables loaded into PostgreSQL")
        print("Tables created:")
        print("- order_details")
        print("- top_customers")
        print("- monthly_revenue")
        print("- popular_categories")
        print("- order_status")
        print("="*50)
        
        return True
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    main()