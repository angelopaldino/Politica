from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
import math
import os
import streamlit as st


def get_data_for_day(input_path, output_path, target_data):
    chunk_size_percent = 2
    # Creazione della sessione Spark con configurazioni per il partizionamento e l'uso della memoria
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
    chunk_size = math.ceil(total_files * (chunk_size_percent / 100.0))  # Calcola la dimensione del chunk

    print(f"Totale file nel dataset: {total_files}")
    print(f"Dimensione del chunk (percentuale): {chunk_size}")

    # Calcola il numero di chunk
    num_chunks = math.ceil(total_files / chunk_size)
    print(f"Numero di chunk da processare: {num_chunks}")

    # Processa i dati in chunk
    for i in range(num_chunks):
        # Estrai i file del chunk corrente
        start_index = i * chunk_size
        end_index = min((i + 1) * chunk_size, total_files)
        chunk_files = input_files[start_index:end_index]
        print(f"Processando chunk {i+1}/{num_chunks}, file da {start_index} a {end_index}")

        try:
            # Leggi i file del chunk in un DataFrame
            df_chunk = spark.read.parquet(*chunk_files)

            # Applica le trasformazioni (filtra per data e formatta created_at)
            df_transformed = df_chunk.withColumn("created_at_str", date_format(col("created_at"), "yyyy-MM-dd")) \
                .filter(col("created_at_str") == target_data)

            # Scrivi il DataFrame trasformato nel percorso di output
            output_chunk_path = os.path.join(output_path, f"chunk_{i+1}.parquet")
            df_transformed.write.parquet(output_chunk_path, mode="overwrite")
            print(f"Scrittura del chunk {i+1} completata: {output_chunk_path}")
        except Exception as e:
            print(f"Errore nel processare il chunk {i+1}: {e}")

    # Carica tutti i file Parquet generati in un unico DataFrame
    try:
        parquet_files = [os.path.join(output_path, f) for f in os.listdir(output_path) if f.endswith(".parquet")]
        if not parquet_files:
            print(f"Errore: Nessun file Parquet trovato nel percorso di output: {output_path}")
            spark.stop()
            return None

        # Carica i file Parquet in un DataFrame
        df = spark.read.parquet(*parquet_files)
        df.show()  # Visualizza il DataFrame completo
        return df

    except Exception as e:
        print(f"Errore nel caricare i file Parquet dal percorso di output: {e}")
        spark.stop()
        return None






