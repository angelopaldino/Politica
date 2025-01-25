from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, to_date, when
import os
import time

def suddividi_per_settimana(input_path, output_path):
    # Crea un'istanza di SparkSession
    spark = SparkSession.builder \
        .appName("BigDataProject") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # Carica il file Parquet
    df = spark.read.parquet(input_path)

    # Converti "created_at" in tipo data
    df = df.withColumn("created_at_date", to_date(df["created_at"], "yyyy-MM-dd"))

    # Calcola la settimana dell'anno
    df = df.withColumn("week", weekofyear(df["created_at_date"]))

    # Gestisci la settimana 5 (se è il 29 ottobre o successivo, metti la settimana 5)
    df = df.withColumn(
        "week",
        when(
            (df["week"] == 5) & (df["created_at_date"] >= "2020-10-29"),
            5
        ).otherwise(df["week"])
    )

    # Genera un nome unico per la cartella di output usando un timestamp
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    unique_output_path = f"{output_path}/output_{timestamp}"

    # Verifica se la cartella di output esiste
    if os.path.exists(unique_output_path):
        print(f"Avviso: La cartella {unique_output_path} esiste già, i dati saranno aggiunti.")
    else:
        print(f"Creazione di una nuova cartella per l'output: {unique_output_path}")

    # Scrivi i dati in Parquet, partizionati per settimana, senza sovrascrivere
    df.write.partitionBy("week").parquet(unique_output_path, mode="append")

    # Facoltativo: se vuoi un singolo file Parquet (coalesce)
    df.coalesce(1).write.parquet(unique_output_path, mode="append")

    # Ferma la sessione Spark
    spark.stop()

