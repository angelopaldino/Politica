import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour, when

def suddividi_per_fascia_oraria(input_path, output_path):
    # Crea un'istanza di SparkSession
    spark = SparkSession.builder \
        .appName("BigDataProject") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # Leggi i dati Parquet
    df = spark.read.parquet(input_path)

    # Aggiungi la colonna "created_at_timestamp" da "created_at"
    df = df.withColumn("created_at_timestamp", to_timestamp(df["created_at"], "yyyy-MM-dd HH:mm:ss"))

    # Estrai l'ora dalla colonna "created_at_timestamp"
    df = df.withColumn("hour", hour(df["created_at_timestamp"]))

    # Crea la colonna "time_of_day" basata sull'ora
    df = df.withColumn(
        "time_of_day",
        when((df["hour"] >= 8) & (df["hour"] < 13), "Morning")  # Mattina: 8-13
        .when((df["hour"] >= 14) & (df["hour"] < 20), "Afternoon")  # Pomeriggio: 14-20
        .when((df["hour"] >= 20) & (df["hour"] < 24), "Evening")  # Sera: 20-24
        .otherwise("Night")  # Notte: 24-7
    )

    # Genera un nome unico per la cartella di output usando un timestamp
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    unique_output_path = f"{output_path}/output_{timestamp}"

    # Verifica se la cartella di output esiste
    if os.path.exists(unique_output_path):
        print(f"Avviso: La cartella {unique_output_path} esiste già, i dati saranno aggiunti.")
    else:
        print(f"Creazione di una nuova cartella per l'output: {unique_output_path}")

    # Scrivi i dati in Parquet, partizionati per fascia oraria, senza sovrascrivere
    df.write.partitionBy("time_of_day").parquet(unique_output_path, mode="append")

    # Facoltativo: se vuoi un singolo file Parquet (coalesce)
    df.coalesce(1).write.parquet(unique_output_path, mode="append")

    # Ferma la sessione Spark
    spark.stop()
