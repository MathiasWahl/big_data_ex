from itertools import islice
from pyspark import SparkConf, SparkContext
import csv

# TASK 4 - friendshipGraph

conf = SparkConf().setAppName("Friendship").setMaster("local");
sc = SparkContext(conf=conf)
folder_name = "./"
input_file_name = "yelp_top_users_friendship_graph.csv"
output_file_name = "results.csv"

results = [["TASK 4"]]

textFile = sc.textFile(folder_name + input_file_name)
friendship_lines_rdd = textFile\
    .mapPartitionsWithIndex(lambda index, line: islice(line, 1, None) if index == 0 else line)\
    .map(lambda line: line.split(","))

# Top 10 nodes with most degrees out (src-nodes)
node_out_degrees_sorted = friendship_lines_rdd.map(lambda fields: (fields[0], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda degree_tuple: degree_tuple[1], False)
node_in_degrees_sorted = friendship_lines_rdd.map(lambda fields: (fields[1], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda degree_tuple: degree_tuple[1], False)

results.append(["4a", "Top 10 nodes out-degrees with count: " + str(node_out_degrees_sorted.take(10))])
results.append(["4a", "Top 10 nodes in-degrees with count: " + str(node_in_degrees_sorted.take(10))])

# Mean and median for number of in/out degrees
total_and_numberof_out_degrees = node_out_degrees_sorted.map(lambda node_tuple: (node_tuple[1], 1)).reduce(lambda x, y: (x[0]+y[0], x[1]+y[1]))
total_and_numberof_in_degrees = node_in_degrees_sorted.map(lambda node_tuple: (node_tuple[1], 1)).reduce(lambda x, y: (x[0]+y[0], x[1]+y[1]))
# Mean
out_degree_nodes = total_and_numberof_out_degrees[1]
in_degree_nodes = total_and_numberof_in_degrees[1]
results.append(["4b", "Average out-degrees per node: " + str(total_and_numberof_out_degrees[0]/out_degree_nodes)])
results.append(["4b", "Average in-degrees per node: " + str(total_and_numberof_in_degrees[0]/in_degree_nodes)])
# Median
out_median = node_out_degrees_sorted.take(int(out_degree_nodes/2))[-1]
in_median = node_in_degrees_sorted.take(int(in_degree_nodes/2))[-1]
results.append(["4b", "Median for out-degrees: " + str(out_median)])
results.append(["4b", "Median for in-degrees: " + str(in_median)])


with open(output_file_name, 'a') as file:
    writer = csv.writer(file)
    writer.writerows(results)
