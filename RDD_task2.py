from itertools import islice
import datetime
from pyspark import SparkConf, SparkContext

# TASK 2 - reviewTable

conf = SparkConf().setAppName("Reviews").setMaster("local");
sc = SparkContext(conf=conf)
folder_name = "./"
input_file_name = "yelp_top_reviewers_with_reviews.csv"
output_file_name = "result_1.txt"

textFile = sc.textFile(folder_name + input_file_name)
review_lines_rdd = textFile\
    .mapPartitionsWithIndex(lambda index, line: islice(line, 1, None) if index == 0 else line)\
    .map(lambda line: line.split())
#review_lines_rdd.cache()

# Disctinct users i dataset
distinct_users_rdd = review_lines_rdd.map(lambda fields: fields[1]).distinct()
print("Number of distinct users: ", distinct_users_rdd.count())

# Average number of characters in user review
review_chars_and_quantity = review_lines_rdd\
    .map(lambda fields: (len(fields[3]), 1))\
    .reduce(lambda review_tuple_1, review_tuple_2:\
        (review_tuple_1[0]+review_tuple_2[0], review_tuple_1[1]+review_tuple_2[1]))

avg_length = float(review_chars_and_quantity[0]/review_chars_and_quantity[1])
print("Average length of reviews: ", avg_length)

# Top 10 businesses with most reviews
business_review_counts = review_lines_rdd.map(lambda fields: (fields[2], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda business_tuple: business_tuple[1], False)
print("Business ID and number of reviews for 10 most reviewed:", business_review_counts.take(10))

# Reviews per year
def unix_to_datetime(time):
    time_in_datetime = datetime.datetime.utcfromtimestamp(float(time))
    return time_in_datetime

reviews_per_year = review_lines_rdd.map(lambda fields: (unix_to_datetime(fields[4]).year, 1)).reduceByKey(lambda x,y: x+y).sortByKey()
print("Reviews per year:", reviews_per_year.collect())

# Time and date for first and last review
last_review = review_lines_rdd.map(lambda fields: (fields[0], float(fields[4]))).reduce(lambda x, y: min(x, y))
print("Review with ID:", last_review[0], " is the last review, created ", unix_to_datetime(last_review[1]))

first_review = review_lines_rdd.map(lambda fields: (fields[0], float(fields[4]))).reduce(lambda x, y: max(x, y))
print("Review with ID:", first_review[0], " is the first review, created ", unix_to_datetime(first_review[1]))

# PCC between number of reviews and avg number of chrs
