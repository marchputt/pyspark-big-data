from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("").getOrCreate()

# Read the CSV file
file_path = "scripts/data/sf-fire-incidents.csv"

# Read the CSV file into a Pandas DataFrame
pandas_df = pd.read_csv(file_path)
print(pandas_df.head())

# Read the CSV file into a Spark DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show()
df.printSchema()

# SQL equivalent:
# SELECT Incident Number, Station Area, Box FROM table
# WHERE box < 5000
df.where(df.Box < 5000).select("Incident Number", "Station Area", "Box").show()

# Stop the Spark session
spark.stop()
