# Add this at the VERY TOP
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, sum, count, date_format, round, when
import os
import subprocess

def setup_environment():
    """Set up Hadoop environment variables"""
    hadoop_path = "C:\\hadoop"
    if os.path.exists(hadoop_path):
        os.environ["HADOOP_HOME"] = hadoop_path
        os.environ["PATH"] = f"{hadoop_path}\\bin;{os.environ['PATH']}"
        print(f"Set HADOOP_HOME to: {hadoop_path}")
    else:
        print("Hadoop directory not found. Some features may not work.")

def create_spark_session():
    """Create and configure Spark session with proper error handling"""
    try:
        spark = SparkSession.builder \
            .appName("EcommercePipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC") \
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC") \
            .config("spark.sql.session.timeZone", "UTC") \
            .master("local[*]") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        print("✅ Spark session created successfully!")
        return spark
        
    except Exception as e:
        print(f"❌ Failed to create Spark session: {e}")
        print("Please check Java installation and set JAVA_HOME")
        raise

def generate_sample_data():
    """Generate sample data using the existing script"""
    try:
        result = subprocess.run([
            "python", "generate_data.py"
        ], capture_output=True, text=True, cwd=".")
        
        if result.returncode == 0:
            print("✅ Sample data generated successfully!")
            return True
        else:
            print(f"❌ Failed to generate data: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error running generate_data.py: {e}")
        return False

def read_csv_with_spark(spark, file_path, schema):
    """Read CSV file with schema validation"""
    try:
        df = spark.read \
            .schema(schema) \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(file_path)
        print(f"✅ Successfully read: {file_path}")
        return df
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        raise

def main():
    print("=== PYSPARK E-COMMERCE PIPELINE ===")
    print("Fixing PySpark configuration issues...")
    
    # Step 1: Setup environment
    setup_environment()
    
    # Step 2: Check and generate data
    print("\n1. 📦 CHECKING SAMPLE DATA...")
    data_files = ["data/customers.csv", "data/products.csv", "data/orders.csv"]
    
    if not all(os.path.exists(f) for f in data_files):
        print("Sample data not found. Generating...")
        if not generate_sample_data():
            print("Please run generate_data.py manually first")
            return
    else:
        print("✅ Sample data exists")
    
    # Step 3: Create Spark session
    print("\n2. 🔧 CREATING SPARK SESSION...")
    try:
        spark = create_spark_session()
    except:
        print("Could not create Spark session. Please check Java installation.")
        return
    
    try:
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
            StructField("order_date", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True)
        ])
        
        # Step 4: Read data
        print("\n3. 📥 READING DATA...")
        customers = read_csv_with_spark(spark, "data/customers.csv", customer_schema)
        products = read_csv_with_spark(spark, "data/products.csv", product_schema)
        orders = read_csv_with_spark(spark, "data/orders.csv", order_schema)
        
        # Rename columns
        customers = customers.withColumnRenamed("name", "customer_name")
        products = products.withColumnRenamed("name", "product_name")
        
        print("\nSample data preview:")
        customers.show(2)
        products.show(2)
        orders.show(2)
        
        # Step 5: Clean data
        print("\n4. 🧹 CLEANING DATA...")
        orders_clean = orders \
            .na.drop() \
            .withColumn("order_date", col("order_date").cast("date")) \
            .filter(col("order_id").isNotNull())
        
        # Step 6: Transform data
        print("\n5. 🔄 TRANSFORMING DATA...")
        order_details = orders_clean \
            .join(customers, "customer_id", "left") \
            .join(products, "product_id", "left") \
            .select(
                col("order_id"),
                col("customer_name"),
                col("country"),
                col("product_name"),
                col("category"),
                col("amount"),
                col("status"),
                col("order_date")
            ) \
            .na.fill({
                'customer_name': 'Unknown Customer',
                'country': 'Unknown',
                'product_name': 'Unknown Product',
                'category': 'Uncategorized',
                'amount': 0.0,
                'status': 'unknown'
            })
        
        print(f"✅ Final records: {order_details.count()}")
        order_details.show(5)
        
        # Create temporary view
        order_details.createOrReplaceTempView("order_details")
        
        # Step 7: Analytics
        print("\n6. 📊 RUNNING ANALYTICS...")
        
        # Query 1: Top customers
        print("💰 TOP CUSTOMERS:")
        top_customers = spark.sql("""
            SELECT customer_name, country, ROUND(SUM(amount), 2) as total_spend
            FROM order_details WHERE status = 'completed'
            GROUP BY customer_name, country ORDER BY total_spend DESC LIMIT 5
        """)
        top_customers.show()
        
        # Query 2: Monthly revenue
        print("📈 MONTHLY REVENUE:")
        monthly_revenue = spark.sql("""
            SELECT date_format(order_date, 'yyyy-MM') as month,
                   ROUND(SUM(amount), 2) as monthly_revenue
            FROM order_details WHERE status = 'completed'
            GROUP BY date_format(order_date, 'yyyy-MM') ORDER BY month DESC
        """)
        monthly_revenue.show()
        
        # Query 3: Popular categories
        print("🏆 POPULAR CATEGORIES:")
        popular_categories = spark.sql("""
            SELECT category, COUNT(*) as order_count
            FROM order_details WHERE status = 'completed'
            GROUP BY category ORDER BY order_count DESC LIMIT 5
        """)
        popular_categories.show()
        
        # Query 4: Order status
        print("📊 ORDER STATUS:")
        order_status = spark.sql("""
            SELECT status, COUNT(*) as order_count,
                   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_details), 2) as percentage
            FROM order_details GROUP BY status ORDER BY order_count DESC
        """)
        order_status.show()
        
        print("\n🎉 PYSPARK PIPELINE COMPLETED SUCCESSFULLY!")
        
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":

    main()
