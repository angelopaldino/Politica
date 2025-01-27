import math
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, max


def analyze_user_activity(input_path, output_path):
    # Creazione della sessione Spark
    spark = SparkSession.builder \
        .appName("User Activity Analysis") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()

    # Verifica che il percorso di input esista
    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        spark.stop()
        return None

    # Ottieni la lista dei file Parquet nel percorso di input
    try:
        input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.parquet')]
        if not input_files:
            print(f"Errore: Nessun file Parquet trovato nel percorso di input: {input_path}")
            spark.stop()
            return None
    except FileNotFoundError:
        print(f"Errore: Percorso di input non trovato: {input_path}")
        spark.stop()
        return None

    total_files = len(input_files)
    chunk_size_percent = 2  # Impostato per esempio, puoi cambiarlo
    chunk_size = math.ceil(total_files * (chunk_size_percent / 100.0))  # Calcola la dimensione del chunk

    # Calcola il numero di chunk
    num_chunks = math.ceil(total_files / chunk_size)
    print(f"Totale file nel dataset: {total_files}")
    print(f"Dimensione del chunk (percentuale): {chunk_size}")
    print(f"Numero di chunk da processare: {num_chunks}")

    # Processa i dati in chunk
    for i in range(num_chunks):
        start_index = i * chunk_size
        end_index = min((i + 1) * chunk_size, total_files)
        chunk_files = input_files[start_index:end_index]
        print(f"Processando chunk {i+1}/{num_chunks}, file da {start_index} a {end_index}")

        try:
            # Leggi i file del chunk in un DataFrame
            df_chunk = spark.read.parquet(*chunk_files)

            # Filtra i dati non validi (nulli o non numerici)
            df_chunk = df_chunk.filter(
                col("retweet_count").isNotNull() &
                col("favorite_count").isNotNull() &
                col("retweet_count").cast("int").isNotNull() &
                col("favorite_count").cast("int").isNotNull()
            )

            # Analisi dei dati:
            # 1. Numero di tweet per utente
            user_tweet_counts = df_chunk.groupBy("user_id_str").agg(count("tweet_id").alias("tweet_count"))

            # 2. Tweet più retwittati per ogni utente
            most_retweeted = df_chunk.groupBy("user_id_str").agg(max("retweet_count").alias("max_retweet_count"))
            most_retweeted = df_chunk.join(most_retweeted, ["user_id_str"], "inner").filter(col("retweet_count") == col("max_retweet_count"))

            # 3. Tweet più favoriti per ogni utente
            most_favorited = df_chunk.groupBy("user_id_str").agg(max("favorite_count").alias("max_favorite_count"))
            most_favorited = df_chunk.join(most_favorited, ["user_id_str"], "inner").filter(col("favorite_count") == col("max_favorite_count"))

            # Scrivi i risultati in file separati per ciascun dataframe
            output_user_tweet_counts = os.path.join(output_path, f"chunk_{i+1}_user_tweet_counts.parquet")
            output_most_retweeted = os.path.join(output_path, f"chunk_{i+1}_most_retweeted.parquet")
            output_most_favorited = os.path.join(output_path, f"chunk_{i+1}_most_favorited.parquet")

            user_tweet_counts.write.parquet(output_user_tweet_counts, mode="overwrite")
            most_retweeted.write.parquet(output_most_retweeted, mode="overwrite")
            most_favorited.write.parquet(output_most_favorited, mode="overwrite")
            print(f"Scrittura dei dati nel chunk {i+1} completata.")

        except Exception as e:
            print(f"Errore nel processare il chunk {i+1}: {e}")

    # Carica tutti i file Parquet generati in un unico DataFrame
    try:
        parquet_files = [os.path.join(output_path, f) for f in os.listdir(output_path) if f.endswith(".parquet")]
        if not parquet_files:
            print(f"Errore: Nessun file Parquet trovato nel percorso di output: {output_path}")
            spark.stop()
            return None

        # Carica i file Parquet in un DataFrame finale
        final_df = spark.read.parquet(*parquet_files)
        # Converti in Pandas DataFrame per Streamlit
        final_df_pd = final_df.toPandas()

        # Visualizza i dati nel terminale (per debug)
        print(final_df_pd.head())

        return final_df_pd

    except Exception as e:
        print(f"Errore nel caricare i file Parquet dal percorso di output: {e}")
        spark.stop()
        return None

