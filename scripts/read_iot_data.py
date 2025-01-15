from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Read IoT Data").getOrCreate()

# Path to the CSV file
csv_file_path = "data/1.csv"

# Read CSV file into DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
