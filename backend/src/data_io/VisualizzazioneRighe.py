import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, lower, explode, split, count

from backend.src.data_io.loader import carica_dati_da_cartelle_annidate


# Funzione per caricare solo la colonna "hashtags"
def carica_hashtags_da_cartelle_annidate(spark, root_directory):
    csv_files = glob.glob(os.path.join(root_directory, "**/*.csv"), recursive=True)
    if not csv_files:
        raise FileNotFoundError(f"Nessun file CSV trovato in {root_directory}")

    # Legge solo la colonna "hashtags"
    df = spark.read.option("header", "true").csv(csv_files).select("hashtags")
    return df

# Funzione per analizzare gli hashtag
def analizza_hashtag(df):
    hashtags_df = df.withColumn("hashtags", regexp_replace(col("hashtags"), "[\\[\\]'\"\s]", "")) \
        .withColumn("hashtags", lower(col("hashtags"))) \
        .withColumn("hashtag", explode(split(col("hashtags"), ","))) \
        .filter(col("hashtag").isNotNull() & (col("hashtag") != "")) \
        .groupBy("hashtag") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc())
    return hashtags_df

# Funzione per salvare il risultato in un file .txt
def salva_in_file(df, output_path):
    df.coalesce(1).write \
        .option("header", "true") \
        .csv(output_path, mode="overwrite")

    # Rinominare il file CSV in .txt
    csv_file = glob.glob(os.path.join(output_path, "*.csv"))[0]
    os.rename(csv_file, os.path.join(output_path, "hashtags_output.txt"))

# Funzione principale
def main():
    spark = SparkSession.builder \
        .appName("BigDataPolitica") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .getOrCreate()

    root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"

    df = carica_dati_da_cartelle_annidate(spark, root_directory)

    df.show(5, truncate=False)

    # Analizza gli hashtag
    #hashtags_df = analizza_hashtag(df)
    #hashtags_df.show(20)

    # Salva il risultato
    #output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Big Data\\output"
    #salva_in_file(hashtags_df, output_path)

    #print(f"Risultati salvati in: {output_path}\\hashtags_output.txt")

    spark.stop()

if __name__ == "__main__":
    main()

