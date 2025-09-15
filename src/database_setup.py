import psycopg2
from psycopg2 import sql
import pandas as pd

print("=== DATABASE SETUP ===")

# Database configuration (update with your actual password)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',  # Connect to default database first
    'user': 'postgres',
    'password': 'Vikas@6084'   # Change this to your actual password
}

def create_database():
    """Create the database if it doesn't exist"""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Required for CREATE DATABASE
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'ecommerce_db'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute("CREATE DATABASE ecommerce_db")
            print("Database 'ecommerce_db' created")
        else:
            print("Database 'ecommerce_db' already exists")
            
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def create_tables():
    """Create tables in the ecommerce_db"""
    try:
        # Update config to use the new database
        config = DB_CONFIG.copy()
        config['database'] = 'ecommerce_db'
        
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        print("Creating tables...")
        
        # Order details table
        cursor.execute("""
        DROP TABLE IF EXISTS order_details;
        CREATE TABLE order_details (
            order_id INTEGER,
            customer_name VARCHAR(255),
            country VARCHAR(100),
            product_name VARCHAR(255),
            category VARCHAR(100),
            amount DECIMAL(10,2),
            status VARCHAR(50),
            order_date DATE
        )
        """)
        
        # Analytics results tables
        cursor.execute("""
        DROP TABLE IF EXISTS top_customers;
        CREATE TABLE top_customers (
            customer_name VARCHAR(255),
            country VARCHAR(100),
            total_spend DECIMAL(10,2)
        )
        """)
        
        cursor.execute("""
        DROP TABLE IF EXISTS monthly_revenue;
        CREATE TABLE monthly_revenue (
            year INTEGER,
            month INTEGER,
            monthly_revenue DECIMAL(10,2)
        )
        """)
        
        cursor.execute("""
        DROP TABLE IF EXISTS popular_categories;
        CREATE TABLE popular_categories (
            category VARCHAR(100),
            order_count INTEGER
        )
        """)
        
        cursor.execute("""
        DROP TABLE IF EXISTS order_status;
        CREATE TABLE order_status (
            status VARCHAR(50),
            order_count INTEGER,
            percentage DECIMAL(5,2)
        )
        """)
        
        conn.commit()
        print("All tables created successfully!")
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

# Run the setup
if create_database():
    create_tables()

print("=== DATABASE SETUP COMPLETED ===")