from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, TimestampType, DoubleType
from pyspark.sql.functions import unbase64, from_unixtime
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("testAppForExercises") \
    .getOrCreate()


# Task 5a
def generate_frames(display_loaded=False, outputs=False):

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

    if outputs:
        to_console="5a:\nData frame objects, showing the columns and types: \n"
        to_console += "Yelp Businesses: " +  str(business_frame) + "\n"
        to_console += "Yelp Top Reviewers: " +  str(top_review_frame) + "\n"
        to_console += "Yelp Top Users Friendship Graph: " +  str(friendship_graph_frame) + "\n"
        print(to_console)

    # Display frames (for testing)
    if display_loaded:
        business_frame.show()
        top_review_frame.show()
        friendship_graph_frame.show()

    return business_frame, top_review_frame, friendship_graph_frame


# Task 6
def do_sql_queries(display_loaded=False, display_queried=False, outputs=False):

    # Generate dataframes
    business_frame, top_review_frame, friendship_graph_frame = generate_frames(display_loaded, outputs)

    # Register the dataframes as SQL temporary views
    business_frame.createOrReplaceTempView("business")
    top_review_frame.createOrReplaceTempView("review")

    # Subtasks a) & b)
    sql_query_df = spark.sql("SELECT * FROM review INNER JOIN business ON review.business_id = business.business_id")
    sql_query_df.createOrReplaceTempView("results")

    # Subtask c)
    top_20_reviewers_df = spark.sql("SELECT COUNT(review_id) as number_of_reviews, user_id FROM review GROUP BY user_id ORDER BY number_of_reviews DESC").limit(20)

    if outputs:
        print("Task 6a:")
        sql_query_df.show()
        print("Task 6b is hard to print. See source code.\nTask 6c:")
        # top_20_reviewers_df.show()


    if display_queried:
        sql_query_df.show()
        top_20_reviewers_df.show()


# FOR RUNNING:

# display the first 20 rows of dataframes loaded from the csv datasets:
display_loaded_frames = False

# display the results from the SQL queries in task 6:
display_queried_frames = False

# Create output for delivery:
output_tasks = True

# Run all the code (do_sql_queries triggers generate_frames)
do_sql_queries(display_loaded=display_loaded_frames, display_queried=display_queried_frames, outputs=output_tasks)
