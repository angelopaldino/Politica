import os
import glob
from pyspark.sql.functions import regexp_replace, col, lower, explode, split, count

def carica_hashtags_da_cartelle_annidate(spark, root_directory):
    csv_files = glob.glob(os.path.join(root_directory, "**/*.csv"), recursive=True)
    if not csv_files:
        raise FileNotFoundError(f"Nessun file CSV trovato in {root_directory}")

    df = spark.read.option("header", "true").csv(csv_files).select("hashtags")
    return df

def analizza_hashtag(df):
    hashtags_df = df.withColumn("hashtags", regexp_replace(col("hashtags"), "[\\[\\]'\"\s]", "")) \
        .withColumn("hashtags", lower(col("hashtags"))) \
        .withColumn("hashtag", explode(split(col("hashtags"), ","))) \
        .filter(col("hashtag").isNotNull() & (col("hashtag") != "")) \
        .groupBy("hashtag") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc())
    return hashtags_df

def salva_in_file(df, output_path):
    df.coalesce(1).write \
        .option("header", "true") \
        .csv(output_path, mode="overwrite")
    csv_file = glob.glob(os.path.join(output_path, "*.csv"))[0]
    os.rename(csv_file, os.path.join(output_path, "hashtags_output.txt"))