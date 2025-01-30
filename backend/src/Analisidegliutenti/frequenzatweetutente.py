import math
import os
import streamlit as st
from pyspark.sql.functions import count, col, max, first

def analyze_user_activity(input_path, output_path):
    if "spark" not in st.session_state:
        raise RuntimeError("Errore: SparkSession non è attiva. Premi 'Avvia App' per iniziarla.")
    else:
        spark = st.session_state.spark

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
    chunk_size = 2  # Per esempio, 100 file per chunk
    num_chunks = math.ceil(total_files / chunk_size)
    start_row=0
    num_rows=15
    print(f"Totale file nel dataset: {total_files}")
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
            user_activity = df_chunk.groupBy("user_id_str").agg(
                count("tweet_id").alias("tweet_count"),  # Conteggio dei tweet per utente
                max("retweet_count").alias("max_retweet_count"),
                max("favorite_count").alias("max_favorite_count"),
                first("text").alias("first_tweet_text")  # Aggiungi il testo del primo tweet per ogni utente
            )

            # Scrivi i risultati in file separati per ciascun dataframe
            output_result = os.path.join(output_path, f"chunk_{i+1}_user_activity.parquet")
            user_activity.write.parquet(output_result, mode="overwrite")
            print(f"Scrittura dei dati nel chunk {i+1} completata.")

        except Exception as e:
            print(f"Errore nel processare il chunk {i+1}: {e}")

    # Carica solo i file richiesti dal percorso di output
    try:
        parquet_files = [os.path.join(output_path, f) for f in os.listdir(output_path) if f.endswith(".parquet")]
        if not parquet_files:
            print(f"Errore: Nessun file Parquet trovato nel percorso di output: {output_path}")
            spark.stop()
            return None

        # Carica i file Parquet in un DataFrame finale
        final_df = spark.read.parquet(*parquet_files)

        # Seleziona solo le colonne necessarie
        final_df_filtered = final_df.select("user_id_str", "tweet_count", "max_retweet_count", "max_favorite_count", "first_tweet_text")

        # Calcola l'indice di inizio e fine in base alla paginazione
        final_df_filtered = final_df_filtered.orderBy("user_id_str").limit(start_row + num_rows).offset(start_row)

        # Converte in Pandas DataFrame per Streamlit
        # Usa toPandas() per convertire il DataFrame Spark in Pandas
        final_df_pd = final_df_filtered.toPandas()

        print("Esempio di dati nel DataFrame Pandas:", final_df_pd.head())
        print("Colonne disponibili:", final_df_pd.columns)

        # Restituisci il DataFrame Pandas
        return final_df_pd

    except Exception as e:
        print(f"Errore nel caricare i file Parquet dal percorso di output: {e}")
        spark.stop()
        return None

