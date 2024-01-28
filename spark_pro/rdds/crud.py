import sys
import time
from pyspark.sql import SparkSession

#Resilient Distributed Datasets (RDDs) in Apache Spark are immutable distributed collections of objects. While RDDs are immutable,
#you can perform operations that simulate CRUD operations by transforming the RDDs using various Spark transformations and actions
#Update (U) Operation: As RDDs are immutable, updating elements would involve creating a new RDD with modified elements.
#Delete (D) Operation: Similarly, deleting elements would involve creating a new RDD excluding the elements to be deleted.

# Initialize Spark session
spark = SparkSession.builder.appName("RDD_CRUD").master("local[*]").getOrCreate()
#spark_pro.sparkContext.setLogLevel("INFO")

##########################  create ##########################
# 1. from collection
data=[1,2,3,4,5]
list_rdd = spark.sparkContext.parallelize(data)
#print(list_rdd.take(5))

#2. from a text file
text_file_path = "../../data/file_a.text"
file_rdd=spark.sparkContext.textFile(text_file_path)
#print(file_rdd.take(1))

#2. from a multiple files  preserving the file path as the key.
test_dir = "../../data"
dir_rdd = spark.sparkContext.wholeTextFiles(test_dir)



##########################  READ/Querying ##########################
#print(dir_rdd.take(2))
#print(dir_rdd.collect())

##########################  UPDATE ##########################
# Map transformation to square each element
squared_rdd = list_rdd.map(lambda x: x**2)
print("Squared RDD:", squared_rdd.collect())

# Filter transformation to keep only even numbers
even_rdd = list_rdd.filter(lambda x: x % 2 == 0)
print("Even Numbers RDD:", even_rdd.collect())


##########################  DELETE ##########################
# Filter operation to exclude specific elements
filtered_rdd = list_rdd.filter(lambda x: x != 3)
print("Filtered RDD (excluding 3):", filtered_rdd.collect())
