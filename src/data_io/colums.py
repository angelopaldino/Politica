from pyspark.sql import SparkSession
#solo per test.
spark2 = SparkSession.builder.appName("ContareColonne").getOrCreate()

# Carica il file CSV come DataFrame
file_path = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\Formatted dataset-20241217T133734Z-001\\Formatted dataset\\2020-10\\tweet_USA_1_october.csv\\tweet_USA_1_october.csv"
df = spark2.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)
numero_colonne = len(df.columns)
print(f"Il file CSV ha {numero_colonne} colonne.")