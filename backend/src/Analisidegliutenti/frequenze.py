import os
import streamlit as st
from pyspark.sql.functions import count, col, max, first, monotonically_increasing_id


def analyze_user_activity2(input_path, output_path):
    if "spark" not in st.session_state:
        raise RuntimeError("Errore: SparkSession non è attiva. Premi 'Avvia App' per iniziarla.")
    else:
        spark = st.session_state.spark


    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        spark.stop()
        return None


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


    try:
        df = spark.read.parquet(*input_files)

        # Filtro i dati non validi (nulli o non numerici)
        df_filtered = df.filter(
            col("retweet_count").isNotNull() &
            col("favorite_count").isNotNull() &
            col("retweet_count").cast("int").isNotNull() &
            col("favorite_count").cast("int").isNotNull()
        )

        # Analisi dei dati:
        user_activity = df_filtered.groupBy("user_id_str").agg(
            count("tweet_id").alias("tweet_count"),
            max("retweet_count").alias("max_retweet_count"),
            max("favorite_count").alias("max_favorite_count"),
            first("text").alias("first_tweet_text")
        )


        output_result = os.path.join(output_path, "user_activity.parquet")
        user_activity.write.parquet(output_result, mode="overwrite")
        print(f"Scrittura dei dati completata nel file {output_result}.")


        final_df = spark.read.parquet(output_result)


        final_df_filtered = final_df.select("user_id_str", "tweet_count", "max_retweet_count", "max_favorite_count", "first_tweet_text")


        final_df_filtered = final_df_filtered.withColumn("row_id", monotonically_increasing_id()) \
            .filter(col("row_id").between(0, 14)) \
            .drop("row_id")


        final_df_pd = final_df_filtered.toPandas()

        print("Esempio di dati nel DataFrame Pandas:", final_df_pd.head())
        print("Colonne disponibili:", final_df_pd.columns)


        return final_df_pd

    except Exception as e:
        print(f"Errore nel processare i dati: {e}")
        spark.stop()
        return None


