from pyspark.sql import SparkSession
import random

# Crea la sessione Spark
spark = SparkSession.builder.appName("SampleDataset").getOrCreate()

# Definisci la directory di input
input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"

# Leggi un sottoinsieme del dataset direttamente in memoria
# L'idea è quella di caricare il dataset in piccole porzioni, senza caricare tutto in memoria
# Usa .option("maxFilesPerTrigger") se i dati sono divisi in più file

# Impostiamo un frazionamento (esempio 10%)
fraction = 0.1  # Percentuale di dati da prelevare (10%)
seed = 42       # Seme per rendere il campionamento riproducibile

# Usa una lettura in streaming per limitare l'uso della memoria
df = spark.read.parquet(input_directory)

# Esegui il campionamento casuale riga per riga
# Utilizzeremo .sample per campionare un certo frazione dei dati
sampled_df = df.sample(fraction=fraction, seed=seed)

# Salva il sottoinsieme in un nuovo file Parquet
subset_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetDataset"
sampled_df.write.mode("overwrite").parquet(subset_path)

# Verifica il numero di righe campionate
print(f"Sottoinsieme casuale salvato in {subset_path}")
