import csv

from pyspark import SparkConf, SparkContext

# Task 3 - Business Table

conf = SparkConf().setAppName("Reviews").setMaster("local")
sc = SparkContext(conf=conf)
folder_name = "./Part 1 - Spark Introduction/"
input_file_name = "./yelp_businesses.csv"
output_file_name = "results.csv"

results = [["TASK 3"]]

textFile = sc.textFile(input_file_name)
header = textFile.first()
business_lines_rdd = textFile\
    .filter(lambda row: row != header)\
    .map(lambda row: row.split('\t'))

# What is the average rating for businesses in each city?
business_average_rating = business_lines_rdd\
    .map(lambda fields: (fields[3], (fields[8], 1)))\
    .reduceByKey(lambda x,y: (int(x[0])+int(y[0]), x[1]+y[1]))\
    .map(lambda tuple: (tuple[0], int(tuple[1][0])/int(tuple[1][1])))

# results.append(['3a', 'Average rating by city: ' + str(business_average_rating.collect())])
results.append(['3a', 'Average rating by city: ' + str(business_average_rating.take(20))])

# What are the top 10 most frequent categories in the data?
business_top_categories = business_lines_rdd\
    .flatMap(lambda fields: fields[10].split(', '))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x+y)\
    .sortBy(lambda x: x[1], False)

results.append(['3b', 'Top 10 most frequent categories: ' + str(business_top_categories.take(10))])

# For each postal code in the business table, calculate the geographical centroid
business_centroid = business_lines_rdd\
    .map(lambda fields: (fields[5], (fields[6], fields[7], 1)))\
    .reduceByKey(lambda x, y: (float(x[0])+float(y[0]), float(x[1])+float(y[1]), x[2]+y[2]))\
    .map(lambda tuple: (tuple[0], (float(tuple[1][0])/tuple[1][2], float(tuple[1][1])/tuple[1][2])))

# results.append(['3c', 'Geographical Centroid for each postal code: ' + str(business_centroid.collect())])
results.append(['3c', 'Geographical Centroid for each postal code: ' + str(business_centroid.take(20))])

with open(folder_name + output_file_name, 'a') as file:
    writer = csv.writer(file)
    writer.writerows(results)
