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

    # a)
    review_users = review_file.map(lambda line: line.split(","))

    print(review_users.count())

rdd_task2()
