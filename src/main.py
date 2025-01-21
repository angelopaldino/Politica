from pyspark.sql import SparkSession
from data_io.loader import carica_dati_da_cartelle_annidate
from src.analysis.hashtag_analysis import carica_hashtags_da_cartelle_annidate, salva_in_file, analizza_hashtag


def main():
    spark = SparkSession.builder \
        .appName("BigDataPolitica") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .getOrCreate()

    root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"

    #df = carica_dati_da_cartelle_annidate(spark, root_directory)

    #df.show(5, truncate=False)


    #WORDCOUNT HASHTAG
    df = carica_hashtags_da_cartelle_annidate(spark, root_directory)
    df.show(5, truncate=False)
    hashtags_df = analizza_hashtag(df)
    hashtags_df.show(20)

    output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Big Data\\output"
    salva_in_file(hashtags_df, output_path)

    print(f"Risultati salvati in: {output_path}\\hashtags_output.txt")


    spark.stop()

if __name__ == "__main__":
    main()


