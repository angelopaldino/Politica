import os
import time
from pyspark.sql import SparkSession

def suddividi_per_giorno(input_path, output_path):
    # Crea un'istanza di SparkSession
    spark = SparkSession.builder \
        .appName("BigDataProject") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()

    # Leggi i dati Parquet
    df = spark.read.parquet(input_path)

    # Aggiungi la colonna "date" a partire dalla colonna "created_at"
    df = df.withColumn("date", df["created_at"].substr(1, 10))

    # Repartiziona i dati su 4 partizioni
    df = df.repartition(4)

    # Genera un nome unico per la cartella di output usando un timestamp
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    unique_output_path = f"{output_path}/output_{timestamp}"

    # Verifica se la cartella di output esiste
    if os.path.exists(unique_output_path):
        print(f"Avviso: La cartella {unique_output_path} esiste già, i dati saranno aggiunti.")
    else:
        print(f"Creazione di una nuova cartella per l'output: {unique_output_path}")

    # Scrivi i dati in Parquet, partizionati per giorno, senza sovrascrivere
    df.write.partitionBy("date").parquet(unique_output_path, mode="append")

    # Facoltativo: se vuoi un singolo file Parquet (coalesce)
    df.coalesce(1).write.parquet(unique_output_path, mode="append")

    # Ferma la sessione Spark
    spark.stop()
