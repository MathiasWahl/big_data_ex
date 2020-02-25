from itertools import islice
import pyspark

reviewTablePath = "./yelp_top_reviewers_with_reviews.csv"

conf = pyspark.SparkConf().setAppName("TDT4305-Project1").setMaster("local");
# The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
sc = pyspark.SparkContext(conf=conf)

def create_rdd(path):
    return sc.textFile(path)

# TASK 2 - reviewTable
def rdd_task2():
    review_file = create_rdd(reviewTablePath)
    print(review_file.first())
    review_file = review_file.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

    # a)
    # review_user_id = review_file.map(lambda line: line.split()[1])
    # print("Number of distinct users: ", review_user_id.distinct().count())

    # b)
    review_text = review_file.map(lambda line: line.split()[3])
    reviews_length = review_text.map(lambda review: len(review))

    total = reviews_length.reduce(lambda x, y: x + y)
    print(total)
    average_length = total / reviews_length.count()
    print("Average length of reviews: %i" % (average_length))


rdd_task2()
