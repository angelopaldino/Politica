import os
from pyspark.sql import functions as F
import streamlit as st


def f(input_path):
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
    df = spark.read.parquet(*input_files)
    return df

def fascia_utente(input_path):
    df = f(input_path)
    df_filtered = df.filter(
        F.col("user_id_str").isNotNull() &
        F.col("screen_name").isNotNull()  # Corretto
    )
    df_grouped = df_filtered.groupBy("user_id_str", "screen_name") \
        .agg(F.count("*").alias("count")) \
        .orderBy("count", ascending=False)
    return df_grouped

def utenti_piu_taggati(input_path):
    df = f(input_path)
    df_filtered = df.filter(F.col("in_reply_to_screen_name")!="NA")
    df_grouped = df_filtered.groupBy("in_reply_to_screen_name") \
        .agg(F.count("*").alias("count")) \
        .orderBy("count", ascending=False)
    return df_grouped


def post_da_utente(input_path, username=""):

    df = f(input_path)


    df_filtered = df.filter(F.col("screen_name") != "NA")

    if username:

        df_filtered = df_filtered.filter(F.lower(F.col("screen_name")).contains(username.lower()))


    print(f"Numero di righe filtrate: {df_filtered.count()}")

    return df_filtered








