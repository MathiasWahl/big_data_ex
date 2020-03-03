from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("testAppForExercises") \
    .getOrCreate()


# Task 5a
def generate_frames(display=False):

    # Read csv files and generate frames. Because InferSchema is set to true, the format of the values will be kept.
    business_frame = spark.read.format('csv').options(inferSchema='true', header='true', delimiter="	").load('yelp_businesses.csv')
    top_review_frame = spark.read.format('csv').options(header='true', inferSchema='true', delimiter="	").load('yelp_top_reviewers_with_reviews.csv')
    friendship_graph_frame = spark.read.format('csv').options(header='true', inferSchema='true').load('yelp_top_users_friendship_graph.csv')

    # Display frames (for testing)
    if display:
        business_frame.show()
        top_review_frame.show()
        friendship_graph_frame.show()

    return business_frame, top_review_frame, friendship_graph_frame


# Task 6
def do_sql_queries(display=False):

    # Generate dataframes
    business_frame, top_review_frame, friendship_graph_frame = generate_frames()

    # Register the dataframes as SQL temporary views
    business_frame.createOrReplaceTempView("business")
    top_review_frame.createOrReplaceTempView("review")

    # Subtasks a) & b)
    sql_query_df = spark.sql("SELECT * FROM review INNER JOIN business ON review.business_id = business.business_id")
    sql_query_df.createOrReplaceTempView("results")

    # Subtask c)
    top_20_reviewers_df = spark.sql("SELECT COUNT(review_id) as number_of_reviews, user_id FROM review GROUP BY user_id ORDER BY number_of_reviews DESC").limit(20)

    if display:
        sql_query_df.show()
        top_20_reviewers_df.show()


# Run all the code (do_sql_queries triggers generate_frames)
do_sql_queries()
