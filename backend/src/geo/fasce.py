import os
from pyspark.sql import functions as F
import streamlit as st

def f(input_path):
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
    df = spark.read.parquet(*input_files)
    return df

def fascia_linguistica(input_path):
    df = f(input_path)
    df_filtered = df.filter(F.col("lang")!="NA")  # Escludi gli utenti NA
    df_grouped = df_filtered.groupBy("lang") \
        .agg(F.count("*").alias("count")) \
        .orderBy("count", ascending=False)
    return df_grouped

def fascia_geografica(input_path):
    df = f(input_path)
    df_filtered = df.filter(F.col("place_name")!="NA")
    df_grouped = df_filtered.groupBy("place_name") \
        .agg(F.count("*").alias("count")) \
        .orderBy("count", ascending=False)
    return df_grouped