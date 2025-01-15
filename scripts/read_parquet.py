from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Read CSV and Save as Parquet").getOrCreate()

# Read the CSV file
file_path = "scripts/data/sf-fire-incidents.parquet"
df = spark.read.parquet(file_path, header=True, inferSchema=True)
df.printSchema()
df.select("Incident Number").show()

# Stop the Spark session
spark.stop()
