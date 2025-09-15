import os
import subprocess

def test_java():
    print("Testing Java installation...")
    result = subprocess.run(["java", "-version"], capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Java is installed")
        print(result.stderr if result.stderr else result.stdout)
        return True
    else:
        print("❌ Java not found. Please install Java JDK 11")
        return False

def test_spark():
    print("\nTesting PySpark...")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Test") \
            .master("local") \
            .getOrCreate()
        print("✅ PySpark works!")
        spark.stop()
        return True
    except Exception as e:
        print(f"❌ PySpark error: {e}")
        return False

if __name__ == "__main__":
    print("=== SPARK ENVIRONMENT TEST ===")
    java_ok = test_java()
    spark_ok = test_spark()
    
    if java_ok and spark_ok:
        print("\n🎉 Environment is ready for PySpark!")
    else:
        print("\n❌ Please fix the issues above")