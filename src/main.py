from pyspark.sql import SparkSession
from data_io.loader import carica_dati_da_cartelle_annidate

def main():
    # Crea la sessione Spark
    spark = SparkSession.builder \
        .appName("BigDataPolitica") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .getOrCreate()

    # Percorso della cartella principale
    root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"

    # Caricamento dei dati
    df = carica_dati_da_cartelle_annidate(spark, root_directory)

    # Mostra un'anteprima dei dati
    df.show(5)

    spark.stop()

if __name__ == "__main__":
    main()

