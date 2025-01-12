from pyspark.sql import SparkSession
import time

# Initialize Spark session
spark = SparkSession.builder.appName("Read CSV and Save as Parquet").getOrCreate()

# Read the CSV file
file_path = "data/sf-fire-incidents.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
df.select("Incident Number").show()

# Stop the Spark session
spark.stop()
