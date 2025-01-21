from pyspark.sql import SparkSession
from data_io.loader import carica_dati_da_cartelle_annidate

def main():
    spark = SparkSession.builder \
        .appName("BigDataPolitica") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .getOrCreate()

    root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"

    df = carica_dati_da_cartelle_annidate(spark, root_directory)

    df.show(5, truncate=False)


    spark.stop()

if __name__ == "__main__":
    main()


