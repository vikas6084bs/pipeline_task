import pandas as pd
import os

print("=== SIMPLE DATA LOADER ===")
print("Creating CSV files only - no database required")

def check_files():
    """Check if all required files exist"""
    required_files = [
        'data/order_details.csv',
        'data/top_customers.csv', 
        'data/monthly_revenue.csv',
        'data/popular_categories.csv',
        'data/order_status.csv'
    ]
    
    print("Checking for required files...")
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file} - EXISTS")
        else:
            print(f"❌ {file} - MISSING")
            return False
    return True

def show_results():
    """Display the results from CSV files"""
    print("\n📊 ANALYTICS RESULTS:")
    
    # Show top customers
    print("\n💰 TOP 5 CUSTOMERS BY SPEND:")
    top_customers = pd.read_csv('data/top_customers.csv')
    print(top_customers.to_string(index=False))
    
    # Show monthly revenue
    print("\n📈 MONTHLY REVENUE TREND:")
    monthly_revenue = pd.read_csv('data/monthly_revenue.csv')
    print(monthly_revenue.to_string(index=False))
    
    # Show popular categories
    print("\n🏆 POPULAR PRODUCT CATEGORIES:")
    popular_categories = pd.read_csv('data/popular_categories.csv')
    print(popular_categories.to_string(index=False))
    
    # Show order status
    print("\n📊 ORDER STATUS SUMMARY:")
    order_status = pd.read_csv('data/order_status.csv')
    print(order_status.to_string(index=False))

def main():
    # Check if files exist
    if not check_files():
        print("\n❌ Some files are missing. Please run the analytics first.")
        print("Run: python src/final_pipeline.py")
        return
    
    # Show results
    show_results()
    
    print("\n🎉 ALL DATA IS READY IN CSV FILES!")
    print("No database required - everything is in the data/ folder")

if __name__ == "__main__":
    main()