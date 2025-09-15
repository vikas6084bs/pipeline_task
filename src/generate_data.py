import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_sample_data():
    """
    Generates sample e-commerce data for the pipeline and introduces
    realistic data quality issues for cleaning.
    """
    # Create the 'raw' data directory if it doesn't exist
    os.makedirs('../data/raw', exist_ok=True)
    
    # Set a random seed for reproducible results
    np.random.seed(42)
    
    # 1. GENERATE CUSTOMERS DATA
    print("Generating customers data...")
    countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan', 'Brazil']
    customers_data = {
        'customer_id': range(1, 101), # 100 customers
        'name': [f'Customer_{i}' for i in range(1, 101)],
        'email': [f'customer_{i}@example.com' for i in range(1, 101)],
        'country': np.random.choice(countries, 100) # Randomly assign a country
    }
    customers_df = pd.DataFrame(customers_data)
    
    # Introduce Data Quality Issues (To be cleaned later)
    customers_df.loc[5:7, 'country'] = np.nan          # Missing countries
    customers_df.loc[10, 'email'] = 'invalid_email'    # Invalid email format
    customers_df.loc[15, 'name'] = np.nan              # Missing name
    
    # Save to CSV
    customers_df.to_csv('../data/raw/customers.csv', index=False)
    
    
    # 2. GENERATE PRODUCTS DATA
    print("Generating products data...")
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Kitchen', 'Sports', 'Beauty']
    products_data = {
        'product_id': range(1, 51), # 50 products
        'name': [f'Product_{i}' for i in range(1, 51)],
        'category': np.random.choice(categories, 50),
        'price': np.round(np.random.uniform(10, 500, 50), 2) # Random prices between $10 and $500
    }
    products_df = pd.DataFrame(products_data)
    
    # Introduce Data Quality Issues
    products_df.loc[3:5, 'category'] = np.nan  # Missing category
    products_df.loc[8, 'price'] = np.nan       # Missing price
    products_df.loc[12, 'price'] = -99.99      # Invalid negative price
    
    # Save to CSV
    products_df.to_csv('../data/raw/products.csv', index=False)
    
    
    # 3. GENERATE ORDERS DATA
    print("Generating orders data...")
    order_statuses = ['completed', 'cancelled', 'pending']
    orders_data = {
        'order_id': range(1, 1001), # 1000 orders
        'customer_id': np.random.randint(1, 101, 1000), # Random customer ID from 1-100
        'product_id': np.random.randint(1, 51, 1000),   # Random product ID from 1-50
        'order_date': [(datetime.now() - timedelta(days=np.random.randint(0, 365))).strftime('%Y-%m-%d') 
                       for _ in range(1000)], # Random dates in the last year
        'amount': np.random.randint(1, 5, 1000), # Quantity between 1 and 4
        'status': np.random.choice(order_statuses, 1000, p=[0.7, 0.1, 0.2]) # 70% completed, 10% cancelled, 20% pending
    }
    orders_df = pd.DataFrame(orders_data)
    
    # Introduce Data Quality Issues
    orders_df.loc[15:18, 'amount'] = np.nan            # Missing amount
    orders_df.loc[20:22, 'status'] = 'unknown'         # Invalid status
    orders_df.loc[25:27, 'customer_id'] = 999          # Invalid customer ID (non-existent)
    orders_df.loc[30:32, 'product_id'] = 999           # Invalid product ID (non-existent)
    
    # Save to CSV
    orders_df.to_csv('../data/raw/orders.csv', index=False)
    
    print("Sample data generation complete! Check the '../data/raw/' directory.")
    print(f"Generated: {len(customers_df)} customers, {len(products_df)} products, {len(orders_df)} orders.")

# This if statement ensures the function runs only when the script is executed directly,
# not when it's imported into another script.
if __name__ == "__main__":
    generate_sample_data()