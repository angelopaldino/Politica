from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from src.data_io.loader import carica_dati_da_cartelle_annidate
from src.data_io.pulizia_dati import pulisci_dati

spark = SparkSession.builder \
    .appName("BigDataProject") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.cores", "8") \
    .config("spark.driver.cores", "8") \
    .getOrCreate()

root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"
input_directory = root_directory
output_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\BigDataPolitica\\Politica\\data\\processed\\pulito"
batch_size = 15000

def partiziona_e_scrivi_in_fasi(input_directory, output_directory, batch_size):
    df = carica_dati_da_cartelle_annidate(spark, input_directory)

    df_with_index = df.withColumn("row_id", monotonically_increasing_id())

    total_rows = df_with_index.count()
    print(f"Righe totali nel dataset: {total_rows}")

    num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)
    print(f"Il dataset sarà processato in {num_batches} batch.")

    for batch in range(num_batches):
        start_row = batch * batch_size
        end_row = start_row + batch_size

        print(f"Processando batch {batch + 1}/{num_batches}: righe da {start_row} a {end_row}...")

        df_batch = df_with_index.filter((col("row_id") >= start_row) & (col("row_id") < end_row)).drop("row_id")

        df_batch_pulito = pulisci_dati(df_batch)

        df_batch_pulito.write.mode("append").parquet(f"{output_directory}/batch_{batch}.parquet")

        print(f"Batch {batch + 1} scritto con successo.")

partiziona_e_scrivi_in_fasi(input_directory, output_directory, batch_size)

spark.stop()
