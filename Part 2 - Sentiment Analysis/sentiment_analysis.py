from pyspark import SparkConf, SparkContext
from base64 import b64decode

reviewTablePath = "../yelp_top_reviewers_with_reviews.csv"

conf = SparkConf().setAppName("Sentiment Analysis").setMaster("local")
sc = SparkContext(conf=conf)

textFile = sc.textFile(reviewTablePath)
header = textFile.first()
review_lines_rdd = textFile\
    .filter(lambda row: row != header)\
    .map(lambda row: row.split())
review_tuples_rdd = review_lines_rdd.map(lambda row: (row[0], b64decode(row[3])))
review_business_rdd = review_lines_rdd.map(lambda row: (row[0], row[2]))

stopwords = set(line.strip() for line in open('stopwords.txt'))

preprocessed_review_tuples_rdd = review_tuples_rdd\
    .map(lambda row: (row[0], tokenize_review(row[1])))\
    .map(lambda row: (row[0], remove_stopwords(row[1])))


def tokenize_review(review):
    review = review.decode('utf-8').replace("\n", " ").split(' ')
    review = [x.strip(" .!,\t@()#$£&").lower() for x in review]
    return review


def remove_stopwords(review):
    return list(filter(lambda word: len(word) > 1 and word not in stopwords, review))


# Load polarity of each word
afinn_list_raw = [line.strip().split('\t') for line in open('AFINN-111.txt')]
afinn_list = [(line[0], int(line[1])) for line in afinn_list_raw]
afinn = dict(afinn_list)

review_with_polarity_sum_rdd = preprocessed_review_tuples_rdd\
    .map(lambda review: (review[0], sum_review_polarity(review[1])))


def sum_review_polarity(review):
    return sum(map(lambda word: afinn.get(word, 0), review))


# Joining (reviewID, reviewPolarityScore) with (reviewID, businessID)
review_business_polarity_rdd = review_business_rdd.join(review_with_polarity_sum_rdd)

sorted_businesses_on_review_score = review_business_polarity_rdd.map(lambda row: row[1])\
    .reduceByKey(lambda business1, business2: business1+business2)\
    .sortBy(lambda businessTuple: businessTuple[1], False)

k = 10
print("Top %s rated business, with score: " % k, sorted_businesses_on_review_score.take(k))
