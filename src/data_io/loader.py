import os
from pyspark.sql import SparkSession

def carica_dati_multipli(spark: SparkSession, directory_path: str):
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(directory_path)


def carica_dati_da_cartelle_annidate(spark: SparkSession, root_directory: str):
    percorsi_csv = []
    for root, _, files in os.walk(root_directory):
        for file in files:
            if file.endswith(".csv"):
                percorsi_csv.append(os.path.join(root, file))

    if not percorsi_csv:
        raise FileNotFoundError(f"Nessun file CSV trovato nella directory {root_directory}")

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(percorsi_csv)

    return df

