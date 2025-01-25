from pyspark.sql import SparkSession

def visualizzazioneParquet():
    spark = SparkSession.builder \
        .appName("Visualizza dati Parquet") \
        .getOrCreate()

    df = spark.read.parquet("C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset")

    df.show()

    df.show(10)

    df.printSchema()
    print(f"Numero totale di righe: {df.count()}")
    spark.stop()

def visualizzazioneDay(input_path):

    spark = SparkSession.builder \
        .appName("ReadParquet") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    df = spark.read.parquet(input_path)
    df.show(10)

    print(f"Numero di righe nel dataset: {df.count()}")

    df.printSchema()

