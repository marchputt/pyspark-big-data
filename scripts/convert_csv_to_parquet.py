from pyspark.sql import SparkSession
import time

start_time = time.time()

# Initialize Spark session
spark = SparkSession.builder.appName("Read CSV and Save as Parquet").getOrCreate()

# Read the CSV file
csv_file_path = "data/sf-fire-incidents.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Save the DataFrame as a Parquet file
parquet_file_path = "data/sf-fire-incidents.parquet"
df.write.parquet(parquet_file_path)

# Stop the Spark session
spark.stop()

# print the time taken
print("Time taken: %s seconds" % (time.time() - start_time))
