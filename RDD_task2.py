from itertools import islice

import pyspark

reviewTablePath = "./yelp_top_reviewers_with_reviews.csv"
businessTablePath = "./yelp_businesses.csv"
friendshipGraphPath = "./yelp_top_users_friendship_graph.csv"

conf = pyspark.SparkConf().setAppName("TDT4305-Project1").setMaster("local");
# The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
sc = pyspark.SparkContext(conf=conf)


######## RDD Tasks ##########
def create_rdd(path):
    return sc.textFile(path)


# TASK 1
def rdd_task1():
    review_file = create_rdd(reviewTablePath)
    business_file = create_rdd(businessTablePath)
    friendship_file = create_rdd(friendshipGraphPath)

    print("Number of rows:\n reviewTable: %i\n businessTable: %i\n friendshipTable: %i" % (review_file.count(), business_file.count(), friendship_file.count()))

# TASK 2 - reviewTable
def rdd_task2():
    review_file = create_rdd(reviewTablePath)
    review_file = review_file.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

    # a)
    # review_user_id = review_file.map(lambda line: line.split()[1])
    # print("Number of distinct users: ", review_user_id.distinct().count())

    # b)
    reviews = review_file.map(lambda line: line.split()[2])
    reviews_length = reviews.map(lambda review: len(review))

