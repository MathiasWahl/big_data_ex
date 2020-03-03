from itertools import islice
from pyspark import SparkConf, SparkContext

# TASK 4 - friendshipGraph

conf = SparkConf().setAppName("Reviews").setMaster("local");
sc = SparkContext(conf=conf)
folder_name = "./"
input_file_name = "yelp_top_users_friendship_graph.csv"
output_file_name = "results"

results = ["TASK 4 RESULTS"]

textFile = sc.textFile(folder_name + input_file_name)
friendship_lines_rdd = textFile\
    .mapPartitionsWithIndex(lambda index, line: islice(line, 1, None) if index == 0 else line)\
    .map(lambda line: line.split())

results.append(str(friendship_lines_rdd.first()))

result_rdd = sc.parallelize(results)
result_rdd.repartition(1).saveAsTextFile(folder_name + output_file_name)
