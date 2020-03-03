#import pyspark
#
#businessTablePath = "./yelp_businesses.csv"
#conf = pyspark.SparkConf().setAppName("TDT4305-Project1").setMaster("local")
# The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
#sc = pyspark.SparkContext(conf=conf)

######## RDD Tasks ##########
#def create_rdd(path):
#    return sc.textFile(path)

#def rdd_task3():
#    business_file = create_rdd(businessTablePath)
#    print(business_file.first())
#    print('xD'*100)
#
#rdd_task3()

from itertools import islice
from pyspark import SparkConf, SparkContext

# Task 3 - Business Table

conf = SparkConf().setAppName("Reviews").setMaster("local")
sc = SparkContext(conf=conf)
folder_name = "./"
input_file_name = "yelp_businesses.csv"
output_file_name = "results"

results = ["TASK 3 RESULTS"]

textFile = sc.textFile(folder_name + input_file_name)
business_lines_rdd = textFile\
    .mapPartitionsWithIndex(lambda index, line: islice(line, 1, None) if index == 0 else line)\
    .map(lambda line: line.split('\t'))

# What is the average rating for businesses in each city?
business_average_rating = business_lines_rdd\
    .map(lambda fields: (fields[3], (fields[8], 1)))\
    .reduceByKey(lambda x,y: (int(x[0])+int(y[0]), x[1]+y[1]))\
    .map(lambda tuple: (tuple[0], int(tuple[1][0])/int(tuple[1][1])))

results.append(str(business_average_rating.collect()))

# What are the top 10 most frequent categories in the data?
business_top_categories = business_lines_rdd\
    .flatMap(lambda fields: fields[10].split(', '))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x+y)\
    .sortBy(lambda x: x[1], False)

results.append(str(business_top_categories.take(10)))

# For each postal code in the business table, calculate the geographical centroid
business_centroid = business_lines_rdd\
    .map(lambda fields: (fields[5], (fields[6], fields[7], 1)))\
    .reduceByKey(lambda x, y: (float(x[0])+float(y[0]), float(x[1])+float(y[1]), x[2]+y[2]))\
    .map(lambda tuple: (tuple[0], (float(tuple[1][0])/tuple[1][2], float(tuple[1][1])/tuple[1][2])))

results.append(str(business_centroid.collect()))

result_rdd = sc.parallelize(results)
result_rdd.repartition(1).saveAsTextFile(folder_name + output_file_name)
