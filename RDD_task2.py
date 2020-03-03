from itertools import islice
import datetime
import csv
import base64
from pyspark import SparkConf, SparkContext
from pyspark.mllib.stat import Statistics

# TASK 2 - reviewTable

conf = SparkConf().setAppName("Reviews").setMaster("local")
sc = SparkContext(conf=conf)
folder_name = "./"
input_file_name = "yelp_top_reviewers_with_reviews.csv"
output_file_name = "results.csv"

textFile = sc.textFile(folder_name + input_file_name)
review_lines_rdd = textFile\
    .mapPartitionsWithIndex(lambda index, line: islice(line, 1, None) if index == 0 else line)\
    .map(lambda line: line.split())

results = [["TASK 2"]]

# Disctinct users i dataset
distinct_users_rdd = review_lines_rdd.map(lambda fields: fields[1]).distinct()
results.append(["2a","Number of distinct users: " + str(distinct_users_rdd.count())])

# Average number of characters in user review
review_chars_and_quantity = review_lines_rdd\
    .map(lambda fields: (len(base64.b64decode(fields[3])), 1))\
    .reduce(lambda review_tuple_1, review_tuple_2:\
        (review_tuple_1[0]+review_tuple_2[0], review_tuple_1[1]+review_tuple_2[1]))

avg_length = float(review_chars_and_quantity[0]/review_chars_and_quantity[1])
results.append(["2b", "Average length of reviews: " + str(avg_length)])

# Top 10 businesses with most reviews
business_review_counts = review_lines_rdd.map(lambda fields: (fields[2], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda business_tuple: business_tuple[1], False)
results.append(["2c", "Business ID and number of reviews for 10 most reviewed: " + str(business_review_counts.take(10))])

# Reviews per year
def unix_to_datetime(time):
    time_in_datetime = datetime.datetime.utcfromtimestamp(float(time))
    return time_in_datetime

reviews_per_year = review_lines_rdd.map(lambda fields: (unix_to_datetime(fields[4]).year, 1)).reduceByKey(lambda x,y: x+y).sortByKey()
results.append(["2d", "Reviews per year: " + str(reviews_per_year.collect())])

# Time and date for first and last review
def min_date(x, y):
    return y if x[1] > y[1] else x

def max_date(x, y):
    return y if y[1] > x[1] else x

last_review = review_lines_rdd.map(lambda fields: (fields[0], float(fields[4]))).reduce(lambda x, y: min_date(x,y))
results.append(["2e", "Review with ID: " + str(last_review[0]) + " is the last review, created " + str(unix_to_datetime(last_review[1]))])

first_review = review_lines_rdd.map(lambda fields: (fields[0], float(fields[4]))).reduce(lambda x, y: max_date(x,y))
results.append(["2e", "Review with ID: " + str(first_review[0]) + " is the first review, created " + str(unix_to_datetime(first_review[1]))])

# PCC between number of reviews and avg number of chrs
user_review_rdd = review_lines_rdd.map(lambda fields: (fields[1], (1, len(base64.b64decode(fields[3]))))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
number_of_reviews = user_review_rdd.map(lambda user_tuple: user_tuple[1][0])
avg_review_length = user_review_rdd.map(lambda user_tuple: user_tuple[1][1]/user_tuple[1][0])

pearson_correlation = Statistics.corr(number_of_reviews, avg_review_length, 'pearson')
results.append(["2f", "The pearson correlation of number of reviews and avg review length is " + str(pearson_correlation)])


with open(output_file_name, 'a') as file:
    writer = csv.writer(file)
    writer.writerows(results)
