from pyspark.sql.types import StructType, StringType, StructField

schema = StructType([
    StructField("tweet_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("user_id_str", StringType(), True),
    StructField("text", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("retweet_count", StringType(), True),
    StructField("favorite_count", StringType(), True),
    StructField("in_reply_to_screen_name", StringType(), True),
    StructField("source", StringType(), True),
    StructField("retweeted", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("location", StringType(), True),
    StructField("place_name", StringType(), True),
    StructField("place_lat", StringType(), True),
    StructField("place_lon", StringType(), True),
    StructField("screen_name", StringType(), True),
])