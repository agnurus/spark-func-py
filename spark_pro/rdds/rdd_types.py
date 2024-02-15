import time

from pyspark.sql import SparkSession
from spark_pro.init.session import create_spark_session

# Initialize Spark session
spark = create_spark_session("RDD_CRUD")

############################# RDD[String] (CRUD Operations) ####################################

## Create RDD of arrays
array_data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
array_rdd = spark.sparkContext.parallelize(array_data)

# Read Operation
#print("Read Array RDD:", array_rdd.collect())

# Update Operation (Multiply each element by 2)
updated_array_rdd = array_rdd.map(lambda arr: [x * 2 for x in arr])
#print("Updated Array RDD:", updated_array_rdd.collect())

# Delete Operation (Filter out arrays with sum greater than 10)
filtered_array_rdd = array_rdd.filter(lambda arr: sum(arr) <= 10)
#("Filtered Array RDD:", filtered_array_rdd.collect())

############################# RDD of key-value pairs ####################################
# Create
key_value_data = [("apple", 3), ("banana", 5), ("orange", 2)]
key_value_rdd = spark.sparkContext.parallelize(key_value_data)

# Read Operation
#print("Read Key-Value RDD:", key_value_rdd.collect())

# Update Operation (Multiply values by 2)
updated_key_value_rdd = key_value_rdd.map(lambda kv: (kv[0], kv[1] * 2))
#print("Updated Key-Value RDD:", updated_key_value_rdd.collect())

# Delete Operation (Filter out elements with value less than 5)
filtered_key_value_rdd = key_value_rdd.filter(lambda kv: kv[1] >= 5)
#print("Filtered Key-Value RDD:", filtered_key_value_rdd.collect())

############################# RDD of tuples ####################################
# Create RDD of tuples
tuple_data = [(1, "apple"), (2, "banana"), (3, "orange")]
tuple_rdd = spark.sparkContext.parallelize(tuple_data)

# Read Operation
#print("Read Tuple RDD:", tuple_rdd.collect())

# Update Operation (Increment the first element of each tuple)
updated_tuple_rdd = tuple_rdd.map(lambda t: (t[0] + 1, t[1]))
#print("Updated Tuple RDD:", updated_tuple_rdd.collect())

# Delete Operation (Filter out tuples with even first element)
filtered_tuple_rdd = tuple_rdd.filter(lambda t: t[0] % 2 != 0)
#print("Filtered Tuple RDD:", filtered_tuple_rdd.collect())

############################# RDD of Custom Objects  ####################################
def print_rdd_collect(rdd_arr):
    for person in rdd_arr:
        print(f"Name: {person.name}, Age: {person.age}")


# Create RDD of custom objects
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

people_data = [Person("Alice", 25), Person("Bob", 30), Person("Charlie", 22)]
people_rdd = spark.sparkContext.parallelize(people_data)

# Read Operation
print("Read Custom Object RDD:",print_rdd_collect(people_rdd.collect()))


# Update Operation (Increment age by 1)
updated_people_rdd = people_rdd.map(lambda person: Person(person.name, person.age + 1))
print("Updated Custom Object RDD:")
#print("Read Custom Object RDD:",print_rdd_collect(updated_people_rdd.collect()))


# Delete Operation (Filter out people below age 30)
filtered_people_rdd = people_rdd.filter(lambda person: person.age >= 30)
print("Filtered Custom Object RDD:")
print("Read Custom Object RDD:",print_rdd_collect(filtered_people_rdd.collect()))

time.sleep(50)





