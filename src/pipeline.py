from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, sum as spark_sum, count, date_format, round as spark_round, when
import os
from pathlib import Path

def create_spark_session():
    """
    Creates and configures a Spark session.
    """
    # Create Spark session without Hadoop dependency for local development
    spark = SparkSession.builder \
        .appName("EcommerceDataPipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session created successfully!")
    return spark

def read_and_standardize_data(spark):
    """
    Reads the CSV files from the data directory.
    """
    base_dir = Path(__file__).parent.parent
    data_path = base_dir / "data"
    
    # Define schemas
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True)
    ])
    
    product_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    
    order_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("order_date", StringType(), True),  # Read as string first
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ])
    
    print("Reading CSV files...")
    
    try:
        customers_df = spark.read.schema(customer_schema).option("header", "true").csv(str(data_path / "customers.csv"))
        products_df = spark.read.schema(product_schema).option("header", "true").csv(str(data_path / "products.csv"))
        orders_df = spark.read.schema(order_schema).option("header", "true").csv(str(data_path / "orders.csv"))
        
        print(f"Customers: {customers_df.count()} records")
        print(f"Products: {products_df.count()} records")
        print(f"Orders: {orders_df.count()} records")
        
        # Standardize column names
        customers_df = customers_df.withColumnRenamed("name", "customer_name")
        products_df = products_df.withColumnRenamed("name", "product_name")
        
        return customers_df, products_df, orders_df
        
    except Exception as e:
        print(f"Error reading files: {e}")
        print("Make sure the CSV files exist in the data/ directory")
        raise e

def clean_data(customers_df, products_df, orders_df):
    """
    Cleans the data by handling missing values and invalid entries.
    """
    print("Cleaning data...")
    
    # Clean customers
    customers_clean = customers_df.fillna({
        'country': 'Unknown',
        'email': 'unknown@example.com'
    }).withColumn("customer_name", 
                 when(col("customer_name").isNull(), "Unknown Customer")
                 .otherwise(col("customer_name")))
    
    # Clean products
    products_clean = products_df.fillna({
        'category': 'Uncategorized',
        'price': 0.0
    }).withColumn("product_name",
                 when(col("product_name").isNull(), "Unknown Product")
                 .otherwise(col("product_name")))
    
    # Clean orders - handle date conversion and missing values
    orders_clean = orders_df.fillna({
        'amount': 0.0,
        'status': 'unknown'
    }).withColumn("order_date", 
                 when(col("order_date").isNull(), "1970-01-01")
                 .otherwise(col("order_date")))
    
    # Convert date string to date type
    orders_clean = orders_clean.withColumn("order_date", 
                                         col("order_date").cast("date"))
    
    # Handle null customer and product IDs
    orders_clean = orders_clean.fillna({
        'customer_id': -1,
        'product_id': -1
    })
    
    # Filter out invalid orders
    orders_clean = orders_clean.filter(
        col("order_id").isNotNull() &
        col("status").isin(["completed", "cancelled", "pending", "unknown"])
    )
    
    print("Data cleaning completed!")
    return customers_clean, products_clean, orders_clean

def transform_data(customers_df, products_df, orders_df):
    """
    Joins all datasets into a single order_details table.
    """
    print("Transforming data...")
    
    # Join orders with customers
    orders_with_customers = orders_df.join(
        customers_df, 
        orders_df.customer_id == customers_df.customer_id, 
        "left"
    )
    
    # Join with products
    order_details = orders_with_customers.join(
        products_df, 
        orders_with_customers.product_id == products_df.product_id, 
        "left"
    )
    
    # Select final columns
    order_details_final = order_details.select(
        orders_df["order_id"], 
        customers_df["customer_name"], 
        customers_df["country"], 
        products_df["product_name"], 
        products_df["category"], 
        orders_df["amount"], 
        orders_df["status"], 
        orders_df["order_date"]
    )
    
    # Fill any remaining nulls from left joins
    order_details_final = order_details_final.fillna({
        'customer_name': 'Unknown Customer',
        'country': 'Unknown',
        'product_name': 'Unknown Product',
        'category': 'Uncategorized'
    })
    
    print(f"Final order_details table has {order_details_final.count()} records")
    return order_details_final

def perform_analytics(order_details):
    """
    Performs analytical queries using DataFrame operations.
    """
    print("\n=== Step 5: Data Analytics ===")
    
    # Top 5 customers by total spend
    print("1. Top 5 customers by total spend...")
    top_customers_df = order_details.filter(col("status") == "completed") \
        .groupBy("customer_name", "country") \
        .agg(spark_round(spark_sum("amount"), 2).alias("total_spend")) \
        .orderBy(col("total_spend").desc()).limit(5)
    
    # Monthly revenue trend
    print("2. Monthly revenue trend...")
    monthly_revenue_df = order_details.filter(col("status") == "completed") \
        .groupBy(date_format("order_date", "yyyy-MM").alias("month")) \
        .agg(spark_round(spark_sum("amount"), 2).alias("monthly_revenue")) \
        .orderBy("month")
    
    # Most popular categories
    print("3. Most popular product categories...")
    popular_categories_df = order_details.filter(col("status") == "completed") \
        .groupBy("category").agg(count("order_id").alias("order_count")) \
        .orderBy(col("order_count").desc())
    
    # Order status statistics
    print("4. Order status statistics...")
    order_status_df = order_details.groupBy("status") \
        .agg(count("order_id").alias("order_count")) \
        .orderBy(col("order_count").desc())
    
    # Show results
    print("\nTop 5 Customers by Total Spend:")
    top_customers_df.show()
    
    print("\nMonthly Revenue Trend:")
    monthly_revenue_df.show()
    
    print("\nMost Popular Product Categories:")
    popular_categories_df.show()
    
    print("\nOrder Status Statistics:")
    order_status_df.show()
    
    return top_customers_df, monthly_revenue_df, popular_categories_df, order_status_df

def save_results(spark, *dataframes):
    """
    Saves the results to CSV files using Spark's native writer.
    """
    print("\nSaving results to CSV...")
    base_dir = Path(__file__).parent.parent / "data"
    
    # Create output directory if it doesn't exist
    output_dir = base_dir / "analytics_results"
    output_dir.mkdir(exist_ok=True)
    
    names = ["top_customers", "monthly_revenue", "popular_categories", "order_status"]
    
    for i, df in enumerate(dataframes):
        if df is not None and df.count() > 0:
            output_path = output_dir / names[i]
            
            # Use Spark's native CSV writer instead of toPandas()
            df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(str(output_path))
            
            print(f"✅ Saved {names[i]} results to {output_path}")
            
            # Also show the data
            print(f"\n{names[i].replace('_', ' ').title()}:")
            df.show()

def main():
    """Main function to run the pipeline"""
    print("Starting Ecommerce Data Pipeline...")
    
    spark = create_spark_session()
    
    try:
        # Step 1: Read data
        print("\n=== Step 1: Data Ingestion ===")
        customers_df, products_df, orders_df = read_and_standardize_data(spark)
        
        # Show sample of raw data
        print("\nSample of raw data:")
        customers_df.show(3)
        products_df.show(3)
        orders_df.show(3)
        
        # Step 2: Clean data
        print("\n=== Step 2: Data Cleaning ===")
        customers_clean, products_clean, orders_clean = clean_data(customers_df, products_df, orders_df)
        
        # Step 3: Transform data
        print("\n=== Step 3: Data Transformation ===")
        order_details = transform_data(customers_clean, products_clean, orders_clean)
        
        # Show sample of final data
        print("\nSample of final order_details data:")
        order_details.show(10, truncate=False)
        
        # Step 4: Analytics
        print("\n=== Step 4: Data Analytics ===")
        results = perform_analytics(order_details)
        
        # Step 5: Save results
        print("\n=== Step 5: Saving Results ===")
        save_results(spark, *results)
        
        print("\n🎉 Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()