import numpy as np
from pyspark.sql.functions import count, col
import streamlit as st
import os
import pandas as pd

def geo(data_path):
    if "spark" not in st.session_state:
        raise RuntimeError("Errore: SparkSession non è attiva. Premi 'Avvia App' per iniziarla.")
    else:
        spark = st.session_state.spark


    if not os.path.exists(data_path):
        print(f"Errore: Il percorso di input non esiste: {data_path}")
        return None

    try:
        input_files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith('.parquet')]
        if not input_files:
            print(f"Errore: Nessun file Parquet trovato nel percorso di input: {data_path}")
            return None
    except FileNotFoundError:
        print(f"Errore: Percorso di input non trovato: {data_path}")
        return None


    df = spark.read.parquet(*input_files)


    df_geo = df.filter(
        (col("place_lat").isNotNull()) & (col("place_lon").isNotNull())
    ).select("place_name", "place_lat", "place_lon")


    df_grouped = df_geo.groupBy("place_name", "place_lat", "place_lon").agg(
        count("place_name").alias("tweet_count")
    )


    try:

        df_pandas = df_grouped.toPandas()


        df_pandas["place_lat"] = df_pandas["place_lat"].replace('NA', np.nan)
        df_pandas["place_lon"] = df_pandas["place_lon"].replace('NA', np.nan)

        df_pandas["place_lat"] = pd.to_numeric(df_pandas["place_lat"], errors='coerce')
        df_pandas["place_lon"] = pd.to_numeric(df_pandas["place_lon"], errors='coerce')

        print(f"Analisi completata. Restituisco {len(df_pandas)} record.")
        return df_pandas

    except Exception as e:
        print(f"Errore durante l'elaborazione: {e}")
        return None

