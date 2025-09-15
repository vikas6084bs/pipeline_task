import pandas as pd
import sqlite3
from datetime import datetime, timedelta

print("=== ECOMMERCE ANALYTICS ===")
print("Starting data analysis...")

# Load the cleaned data
print("1. Loading order details...")
order_details = pd.read_csv("data/order_details.csv")
order_details['order_date'] = pd.to_datetime(order_details['order_date'])

print(f"   - Loaded {len(order_details)} records")

# Create an in-memory SQLite database for SQL queries
print("\n2. Setting up SQL database...")
conn = sqlite3.connect(':memory:')
order_details.to_sql('order_details', conn, index=False, if_exists='replace')

print("3. Running analytical queries...")

# Query 1: Top 5 customers by total spend
print("\n   Query 1: Top 5 customers by total spend")
query1 = """
SELECT customer_name, country, ROUND(SUM(amount), 2) as total_spend
FROM order_details 
WHERE status = 'completed'
GROUP BY customer_name, country 
ORDER BY total_spend DESC 
LIMIT 5
"""
top_customers = pd.read_sql_query(query1, conn)
print(top_customers.to_string(index=False))

# Save to CSV
top_customers.to_csv("data/top_customers.csv", index=False)

# Query 2: Monthly revenue trend (last 6 months)
print("\n   Query 2: Monthly revenue trend")
six_months_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
query2 = f"""
SELECT 
    strftime('%Y-%m', order_date) as month,
    ROUND(SUM(amount), 2) as monthly_revenue
FROM order_details 
WHERE status = 'completed' 
    AND order_date >= '{six_months_ago}'
GROUP BY strftime('%Y-%m', order_date)
ORDER BY month DESC
"""
monthly_revenue = pd.read_sql_query(query2, conn)
print(monthly_revenue.to_string(index=False))

# Save to CSV
monthly_revenue.to_csv("data/monthly_revenue.csv", index=False)

# Query 3: Most popular product categories
print("\n   Query 3: Most popular product categories")
query3 = """
SELECT category, COUNT(*) as order_count
FROM order_details 
WHERE status = 'completed'
GROUP BY category 
ORDER BY order_count DESC
LIMIT 5
"""
popular_categories = pd.read_sql_query(query3, conn)
print(popular_categories.to_string(index=False))

# Save to CSV
popular_categories.to_csv("data/popular_categories.csv", index=False)

# Query 4: Order status summary
print("\n   Query 4: Order status summary")
query4 = """
SELECT 
    status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_details), 2) as percentage
FROM order_details 
GROUP BY status 
ORDER BY order_count DESC
"""
order_status = pd.read_sql_query(query4, conn)
print(order_status.to_string(index=False))

# Save to CSV
order_status.to_csv("data/order_status.csv", index=False)

# Close connection
conn.close()

print("\n4. Saving all analytics results...")
print("   - top_customers.csv")
print("   - monthly_revenue.csv") 
print("   - popular_categories.csv")
print("   - order_status.csv")

print("\n=== ANALYTICS COMPLETED SUCCESSFULLY ===")