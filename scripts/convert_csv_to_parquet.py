from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Read CSV and Save as Parquet").getOrCreate()

# Read the CSV file
csv_file_path = "scripts/data/sf-fire-incidents.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Save the DataFrame as a Parquet file
parquet_file_path = "scripts/data/sf-fire-incidents.parquet"
df.write.parquet(parquet_file_path)

# Stop the Spark session
spark.stop()
