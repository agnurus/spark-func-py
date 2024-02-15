
from pyspark.sql import SparkSession

from spark_pro.init.session import create_spark_session

# Initialize Spark session
spark = create_spark_session("RDD_CRUD")

array_data = [1, 2, 3,4, 5, 6,7, 8, 9]
array_rdd = spark.sparkContext.parallelize(array_data)

#tranformnations
array_rdd.map(lambda x: x*x)



#advance
#array_rdd.aggregate()

# actions
#for loop
def print_element(element):
    print(element)
array_rdd.foreach(lambda elem: print_element(elem))
# or array_rdd.foreach(print_element)

def print_partition(partition):
    for elem in partition:
        print(elem)
#for loop with in partition
array_rdd.foreachPartition(lambda partition:print_partition(partition))






