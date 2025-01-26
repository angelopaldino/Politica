from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, explode, split, lower, regexp_extract, to_date, hour, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType
from datetime import datetime

def main() :
    spark_session = create_spark_session(app_name="Test")
    dataset = leggi_dataset(spark_session)

    data = post_da_utente(dataset=dataset, username="KamalaHarris")
    data.show(truncate=False)

    spark_session.stop()





def create_spark_session(app_name=str):
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", "14g")
            .config("spark.executor.memory", "14g")
            .getOrCreate())



def leggi_dataset(spark: SparkSession):
    percorso_parquet = "dataset/"  

    schema = StructType([
        StructField("tweet_id", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("user_id_str", StringType(), True),
        StructField("text", StringType(), True),
        StructField("hashtags", StringType(), True),
        StructField("retweet_count", LongType(), True),
        StructField("favorite_count", LongType(), True),
        StructField("in_reply_to_screen_name", StringType(), True),
        StructField("source", StringType(), True),
        StructField("retweeted", BooleanType(), True),
        StructField("lang", StringType(), True),
        StructField("location", StringType(), True),
        StructField("place_name", StringType(), True),
        StructField("place_lat", StringType(), True),
        StructField("place_lon", StringType(), True),
        StructField("screen_name", StringType(), True),
    ])

    dataset = spark.read.parquet(percorso_parquet, schema=schema)
    return dataset



def word_count(dataset: DataFrame):

    words = (dataset
        .select(col("text"))
        .withColumn("word", explode(split(lower(col("text")), "\\s+")))
        .filter(~col("word").startswith("http"))
        .filter(~col("word").startswith("@"))
        .filter(~col("word").rlike("[^a-zA-Z]"))
        .filter(col("word") != "") 
        .groupBy("word")
        .agg(count("*").alias("count"))
        .orderBy("count", ascending=False))

    return words

def fascia_oraria(dataset: DataFrame):
    hours = (dataset
        .select(col("created_at"))
        .withColumn("hour", regexp_extract(col("created_at"), r"(?<=\s)\d{2}(?=:)", 0))
        .groupBy("hour")                                                               
        .agg(count("*").alias("count"))                                                 
        .orderBy("hour", ascending=True))   
    
    return hours

def fascia_giornaliera(dataset: DataFrame):
    hours = (dataset
        .select(col("created_at"))
        .withColumn("day", regexp_extract(col("created_at"), r"\d{4}-\d{1,2}-(\d{1,2})", 0))
        .groupBy("day")                                                               
        .agg(count("*").alias("count"))                                                 
        .orderBy("day", ascending=True))   
    
    return hours

def fascia_oraria_giorno(dataset: DataFrame, data: str):

    data_obj = datetime.strptime(data, "%Y-%m-%d").date()

    hours_day = (dataset
        .withColumn("data_ts", to_timestamp(col("created_at")))
        .filter(to_date(col("data_ts")) == data_obj)
        .withColumn("hour", hour(col("data_ts")))
        .groupBy("hour")
        .count()
        .orderBy("hour")
    )

    return hours_day   

def fascia_linguistica(dataset: DataFrame):

    language = (dataset
        .select(col("lang"))
        .groupBy("lang")                                                               
        .agg(count("*").alias("count"))                                                 
        .orderBy("lang", ascending=True))  

    return language

def fascia_geografica(dataset: DataFrame):

    location = (dataset
        .select(col("location"))
        .groupBy("location")                                                               
        .agg(count("*").alias("count"))                                                 
        .orderBy("location", ascending=True))  

    return location

def fascia_utente(dataset: DataFrame):

    user = (dataset
        .groupBy("user_id_str", "screen_name")                                                               
        .agg(count("*").alias("count"))                                                 
        .orderBy("count", ascending=False))  

    return user

def post_piu_retweettati(dataset: DataFrame):
    post = (dataset
        .withColumn("favorite_count", col("retweet_count").cast("long"))
        .groupBy("user_id_str", "screen_name", "retweet_count", "text")
        .agg()                                                 
        .orderBy("retweet_count", ascending=False))  

    return post

def post_piu_like(dataset: DataFrame):
    post = (dataset
        .withColumn("favorite_count", col("favorite_count").cast("long"))
        .groupBy("user_id_str", "screen_name", "favorite_count", "text")    
        .agg()                                             
        .orderBy("favorite_count", ascending=False))  

    return post

def utenti_piu_taggati(dataset: DataFrame):
    post = (dataset
        .filter(col("in_reply_to_screen_name") != "NA")  
        .groupBy("in_reply_to_screen_name")
        .agg(count("*").alias("count"))                                             
        .orderBy("count", ascending=False))  

    return post

def utenti_piu_retweettati(dataset: DataFrame):
    post = (dataset
        .withColumn("favorite_count", col("favorite_count").cast("long"))
        .groupBy("user_id_str", "screen_name", "favorite_count")
        .agg(count("*").alias("count"))                                                   
        .orderBy("retweet_count", ascending=False))  

    return post

def utenti_piu_like(dataset: DataFrame):
    post = (dataset
        .withColumn("favorite_count", col("favorite_count").cast("long"))
        .groupBy("favorite_count")    
        .agg(count("*").alias("count"))                                            
        .orderBy("favorite_count", ascending=False))  

    return post

def post_da_utente(dataset: DataFrame, username: str):
    post = (dataset
        .filter(col("screen_name") == username) 
        .groupBy("user_id_str", "screen_name", "text")  
        .agg(count("*").alias("count"))                                            
        .orderBy("screen_name", ascending=False))  

    return post

if __name__ == "__main__":
    main()