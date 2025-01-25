from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

def subset(input, output):
    spark = SparkSession.builder \
        .appName("Partial Loading Example") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    # Aggiungi un indice al dataset
    window = Window.orderBy("tweet_id")  # Usa una colonna univoca come "tweet_id"
    df_with_index = spark.read.parquet(input).withColumn("row_number", row_number().over(window))

    batch_size = 1000000  # Righe per batch
    max_index = df_with_index.selectExpr("max(row_number)").collect()[0][0]

    # Per ottimizzare le prestazioni, possiamo usare una partizione basata su "row_number"
    # Questo farà sì che i dati siano distribuiti su più partizioni anziché accumularsi in una sola
    num_partitions = 100  # Imposta il numero di partizioni desiderate, ad esempio 100

    # Ricalcoliamo il DataFrame con la partizione
    df_with_index = df_with_index.repartition(num_partitions)

    for start in range(1, max_index, batch_size):
        end = start + batch_size
        batch_df = df_with_index.filter((df_with_index.row_number >= start) & (df_with_index.row_number < end))

        # Processa e salva il batch
        batch_df.write.mode("append").parquet(output)

    spark.stop()



