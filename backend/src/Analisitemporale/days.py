from pyspark.sql.functions import col, date_format
import os
import streamlit as st

def get_data_for_day2(input_path, output_path, target_data):
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

    # Leggi direttamente tutti i file Parquet in un unico DataFrame
    try:
        df = spark.read.parquet(*input_files)

        # Applica le trasformazioni (filtra per data e formatta created_at)
        df_transformed = df.withColumn("created_at_str", date_format(col("created_at"), "yyyy-MM-dd")) \
            .filter(col("created_at_str") == target_data)

        # Scrivi il DataFrame trasformato nel percorso di output
        df_transformed.write.parquet(output_path, mode="overwrite")
        print(f"Scrittura del DataFrame trasformato completata: {output_path}")

        return df_transformed

    except Exception as e:
        print(f"Errore nel caricare e trasformare i dati: {e}")
        spark.stop()
        return None
