import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


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

def carica_dati_da_cartelle_annidate(spark: SparkSession, root_directory: str):
    percorsi_csv = []


    for root, _, files in os.walk(root_directory):
        for file in files:
            if file.endswith(".csv"):
                percorsi_csv.append(os.path.join(root, file))


    if not percorsi_csv:
        raise FileNotFoundError(f"Nessun file CSV trovato nella directory {root_directory}")


    return spark.read \
        .option("delimiter", ",") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("header", "true") \
        .option("multiline", "true") \
        .option("mode", "DROPMALFORMED") \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .schema(schema) \
        .csv(percorsi_csv)




def carica_singolo_file(spark: SparkSession, file_path: str):
    return spark.read \
        .option("delimiter", ",") \
        .option("header", "true") \
        .option("multiline", "true") \
        .option("mode", "DROPMALFORMED") \
        .schema(schema) \
        .csv(file_path)

# Crea la sessione Spark
spark = SparkSession.builder \
    .appName("Caricamento Dati Twitter") \
    .getOrCreate()

# Percorso della directory contenente i file CSV
root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\Formatted dataset-20241217T133734Z-001\\Formatted dataset\\2020-10"





