import pyspark
import csv

reviewTablePath = "./yelp_top_reviewers_with_reviews.csv"
businessTablePath = "./yelp_businesses.csv"
friendshipGraphPath = "./yelp_top_users_friendship_graph.csv"
output_file_name = "results.csv"


conf = pyspark.SparkConf().setAppName("Load Data").setMaster("local")
sc = pyspark.SparkContext(conf=conf)

######## RDD Tasks ##########
def create_rdd(path):
    return sc.textFile(path)

results = [["TASK 1"]]

# TASK 1
def rdd_task1():
    review_file = create_rdd(reviewTablePath)
    business_file = create_rdd(businessTablePath)
    friendship_file = create_rdd(friendshipGraphPath)

    results.append(["1", "Number of rows:\n reviewTable: %i\n businessTable: %i\n friendshipTable: %i" % (review_file.count(), business_file.count(), friendship_file.count())])

rdd_task1()

with open(output_file_name, 'a') as file:
    writer = csv.writer(file)
    writer.writerows(results)
