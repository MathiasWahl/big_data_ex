
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Exercises") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


# Task 5a
def generate(display=False):
    # read csv files and generate Frames
    business_frame = spark.read.format('csv').options(inferSchema='true', header='true', delimiter="	").load('yelp_businesses.csv')
    top_review_frame = spark.read.format('csv').options(header='true', inferSchema='true', delimiter="	").load('yelp_top_reviewers_with_reviews.csv')
    friendship_graph_frame = spark.read.format('csv').options(header='true', inferSchema='true').load('yelp_top_users_friendship_graph.csv')

    # display frames
    if display:
        business_frame.show()
        top_review_frame.show()
        friendship_graph_frame.show()

    return business_frame, top_review_frame, friendship_graph_frame


# Task 6
def sql_queries(display=False):
    business_frame, top_review_frame, friendship_graph_frame = generate()

    business_frame.createOrReplaceTempView("business")
    top_review_frame.createOrReplaceTempView("review")

    # a) & b)
    sql_query_df = spark.sql("SELECT * FROM review INNER JOIN business ON review.business_id = business.business_id")
    sql_query_df.createOrReplaceTempView("results")

    # c)

    ouff = spark.sql("SELECT COUNT(review_id) as number_of_reviews, user_id FROM review GROUP BY user_id ORDER BY number_of_reviews DESC").limit(20)
    ouff.show()

    # Find the number of reviews for each user in the review table for
    # top 20 users with the most number of reviews sorted
    # descendingly



    if display:
        sql_query_df.show()


sql_queries(False)
