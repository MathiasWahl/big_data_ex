from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, TimestampType, DoubleType
from pyspark.sql.functions import unbase64, from_unixtime
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("testAppForExercises") \
    .getOrCreate()


# Task 5a
def generate_frames(display=False):

    # Read csv files, specify column types and generate frames
    business_schema = StructType()\
        .add("business_id", StringType(), True)\
        .add("name", StringType(), True)\
        .add("address", StringType(), True)\
        .add("city", StringType(), True)\
        .add("state", StringType(), True)\
        .add("postal_code", StringType(), True)\
        .add("latitude", FloatType(), True)\
        .add("longitude", FloatType(), True)\
        .add("stars", FloatType(), True)\
        .add("review_count", IntegerType(), True)\
        .add("categories", StringType(), True)

    business_frame = spark.read.format('csv')\
        .options(header='true', delimiter="	")\
        .schema(business_schema)\
        .load('yelp_businesses.csv')

    top_review_schema = StructType() \
        .add("review_id", StringType()) \
        .add("user_id", StringType()) \
        .add("business_id", StringType()) \
        .add("review_text", StringType()) \
        .add("review_date", StringType()) # Will be converted to TimestampType

    top_review_frame = spark.read.format('csv')\
        .options(header='true', delimiter="	") \
        .schema(top_review_schema) \
        .load('yelp_top_reviewers_with_reviews.csv')

    # Decode review_text, convert review_date to TimestampType
    top_review_frame = top_review_frame\
        .withColumn('review_text', unbase64(top_review_frame.review_text).cast(StringType()))\
        .withColumn('review_date', from_unixtime(top_review_frame.review_date).cast(TimestampType()))

    friendship_graph_frame = spark.read.format('csv')\
        .options(header='true', inferSchema='true')\
        .load('yelp_top_users_friendship_graph.csv')

    output = ['5a', "Dataframes, their column names and types: ", str(business_frame) + str(top_review_frame) + str(friendship_graph_frame)]
    print(output)

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
