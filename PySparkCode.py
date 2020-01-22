# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

# Create my_spark 
# If one exsit use it if not create one
spark = SparkSession.builder.getOrCreate()

# Print my_spark Make sure it is a pyspark session
print(spark)


# Print the tables in the catalog
print(spark.catalog.listTables())

# SQL Query get the first 10 flights
query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()

###
#Convert the Spark query into a local pandas dataframe
###
# Don't change this query
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())

###
# Send a dataframe to a spark tables
###
# Create pd_temp Random numbers
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog Show that new dataframe is not avaliable on spark
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())

###
# Read in data into spark dataframe from a csv file
###
# Don't change this file path
file_path = "/usr/local/share/datasets/airports.csv"

# Read in the airports data Take column names as the first line in csv
airports = spark.read.csv(file_path, header=True)

# Show the data
airports.show()




