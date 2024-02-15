from spark_pro.init.session import create_spark_session

# Initialize Spark session
spark = create_spark_session("RDD_CRUD")

array_data = [1, 2, 3, 4, 5, 6,7, 8, 9]
array_rdd = spark.sparkContext.parallelize(array_data)

#tranformnations
array_rdd.map(lambda x: x*x)