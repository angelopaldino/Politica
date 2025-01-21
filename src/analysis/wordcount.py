from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count, regexp_replace, udf
from pyspark.sql.types import BooleanType


import nltk
from nltk.corpus import words as nltk_words
from nltk.corpus import names as nltk_names


import os
from concurrent.futures import ThreadPoolExecutor, as_completed

nltk.download("words")
nltk.download("names")

english_words = set(nltk_words.words())
proper_names = set(nltk_names.words())

valid_words = english_words.union(proper_names)
percorso_file="C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\Formatted dataset-20241217T133734Z-001\\Formatted dataset\2020-10\\tweet_USA_1_october.csv\\tweet_USA_1_october.csv\\"

def is_valid_word(word):
    return word in valid_words

def is_english(word):
    return word in english_words

is_valid_word = udf(is_valid_word, BooleanType())



def word_count(spark_session: SparkSession, percorso_file: str):

    data = (spark_session.read \
        .format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", "true")
        .option("mode", "DROPMALFORMED")
        .option("inferSchema", "true")
        .load(percorso_file))

    words = (data
        .select(col("text"))
        .withColumn("word", explode(split(lower(col("text")), "\\s+")))
        .filter(~col("word").startswith("http"))
        .filter(~col("word").startswith("@"))
        .filter(~col("word").rlike("[^a-zA-Z]"))
        .filter(col("word") != "") 
        .filter(is_valid_word(col("word")))
        .groupBy("word")
        .agg(count("*").alias("count"))
        .orderBy("count", ascending=False))
    
    return words




def multi_word_count(spark_session: SparkSession):

    
    cartella_input = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\Formatted dataset-20241217T133734Z-001\\Formatted dataset\\2020-10\\tweet_USA_1_october.csv\\"
    cartella_output = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\output\\"

    files = [os.path.join(cartella_input, f"tweet_USA_{i}_october.csv") for i in range(1, 32)]

    def process_file(file):
        return word_count(spark_session=spark_session, percorso_file=file)

    all_results = None

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, file) for file in files]
        
        for future in as_completed(futures):
            word_counts = future.result()
            if all_results is None:
                all_results = word_counts
            else:
                all_results = all_results.union(word_counts)

    
    final_results = all_results.groupBy("word").agg({"count": "sum"}) \
        .withColumnRenamed("sum(count)", "total_count") \
        .orderBy("total_count", ascending=False)

    final_results.coalesce(1).write.mode("overwrite").option("header", "true").csv(cartella_output)