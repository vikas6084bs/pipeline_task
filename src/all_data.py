import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

print("=== CREATING ALL REQUIRED DATA FILES ===")

# Create data folder if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')
    print("Created data/ folder")

# 1. Generate sample data
print("1. Generating sample data...")

# Customers
customers_data = []
for i in range(1, 101):
    customers_data.append({
        'customer_id': i,
        'name': f'Customer_{i}',
        'email': f'customer{i}@example.com',
        'country': random.choice(['USA', 'UK', 'Canada', 'Australia'])
    })

# Products
products_data = []
for i in range(1, 21):
    products_data.append({
        'product_id': i,
        'name': f'Product_{i}',
        'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home']),
        'price': round(random.uniform(10, 100), 2)
    })

# Orders
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
print("✅ Raw data files created")

# 2. Clean and join data
print("2. Cleaning and joining data...")
customers = pd.read_csv('data/customers.csv')
products = pd.read_csv('data/products.csv')
orders = pd.read_csv('data/orders.csv')

# Clean data
orders_clean = orders.dropna()
orders_clean['order_date'] = pd.to_datetime(orders_clean['order_date'])

# Join data
merged = orders_clean.merge(customers, on='customer_id', how='left')
order_details = merged.merge(products, on='product_id', how='left', suffixes=('_order', '_product'))

# Create final table
order_details = order_details[[
    'order_id', 'name_order', 'country', 'name_product', 
    'category', 'amount', 'status', 'order_date'
]]
order_details.columns = [
    'order_id', 'customer_name', 'country', 'product_name',
    'category', 'amount', 'status', 'order_date'
]

order_details.to_csv('data/order_details.csv', index=False)
print("✅ order_details.csv created")

# 3. Create analytics files
print("3. Creating analytics files...")

# Top customers
top_customers = order_details[order_details['status'] == 'completed']
top_customers = top_customers.groupby(['customer_name', 'country'])['amount'].sum().reset_index()
top_customers.columns = ['customer_name', 'country', 'total_spend']
top_customers = top_customers.sort_values('total_spend', ascending=False).head(5)
top_customers.to_csv('data/top_customers.csv', index=False)

# Monthly revenue
monthly_revenue = order_details[order_details['status'] == 'completed']
monthly_revenue['year'] = pd.to_datetime(monthly_revenue['order_date']).dt.year
monthly_revenue['month'] = pd.to_datetime(monthly_revenue['order_date']).dt.month
monthly_revenue = monthly_revenue.groupby(['year', 'month'])['amount'].sum().reset_index()
monthly_revenue.columns = ['year', 'month', 'monthly_revenue']
monthly_revenue.to_csv('data/monthly_revenue.csv', index=False)

# Popular categories
popular_categories = order_details[order_details['status'] == 'completed']
popular_categories = popular_categories.groupby('category').size().reset_index()
popular_categories.columns = ['category', 'order_count']
popular_categories = popular_categories.sort_values('order_count', ascending=False)
popular_categories.to_csv('data/popular_categories.csv', index=False)

# Order status
order_status = order_details.groupby('status').size().reset_index()
order_status.columns = ['status', 'order_count']
order_status['percentage'] = (order_status['order_count'] / order_status['order_count'].sum() * 100).round(2)
order_status.to_csv('data/order_status.csv', index=False)

print("✅ All analytics files created")
print("\n📁 FILES CREATED IN data/ FOLDER:")
files = os.listdir('data')
for file in files:
    if file.endswith('.csv'):
        print(f"   - {file}")

print("\n🎉 All data files are ready! Now you can run your PostgreSQL loader.")