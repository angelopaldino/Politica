from pyspark.sql.functions import udf, col, split, explode
from pyspark.sql.types import StringType
import re
import math
import os
from pyspark.sql import SparkSession
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

# Funzione per rimuovere i simboli HTML e normalizzare il testo
def clean_text(text):
    # Rimuove i simboli HTML come &amp;, &lt;, &gt;, etc.
    text = re.sub(r'&\w+;', '', text)
    # Rimuove eventuali caratteri di ritorno a capo o spazi extra
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Registra la funzione UDF in Spark
clean_text_udf = udf(clean_text, StringType())

# Carica le stop words in inglese
stop_words = set(stopwords.words('english'))

def count_words_for_candidate_in_chunks(input_path, candidate_keywords):
    chunk_size_percent = 3  # Aumenta la dimensione del chunk
    spark = SparkSession.builder \
        .appName("CountsWords") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()

    # Verifica che il percorso di input esista
    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        spark.stop()
        return None

    # Ottieni la lista dei file Parquet nel percorso di input
    input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.parquet')]
    total_files = len(input_files)
    chunk_size = math.ceil(total_files * (chunk_size_percent / 100.0))  # Calcola la dimensione del chunk
    num_chunks = math.ceil(total_files / chunk_size)

    # Processa i dati in chunk
    for i in range(num_chunks):
        start_index = i * chunk_size
        end_index = min((i + 1) * chunk_size, total_files)
        chunk_files = input_files[start_index:end_index]

        try:
            df_chunk = spark.read.parquet(*chunk_files)

            # Verifica la presenza della colonna 'text'
            if "text" not in df_chunk.columns:
                print(f"Errore: La colonna 'text' non è presente nel chunk {i+1}")
                continue

            # Estrai e pulisci il testo, poi filtra i tweet che menzionano i candidati
            df_filtered = filter_tweets_by_candidate(df_chunk, candidate_keywords)

            # Se ci sono tweet dopo il filtro, analizza il testo
            if df_filtered.count() > 0:
                # Applica la pulizia del testo direttamente alla colonna 'text'
                df_words = df_filtered.withColumn("cleaned_text", clean_text_udf(col("text")))

                # Estrai le parole dalla colonna 'cleaned_text' pulita
                df_words = df_words.withColumn("words", explode(split(col("cleaned_text"), " ")))

                # Filtra le stop words dalla colonna 'words'
                df_words_filtered = df_words.filter(~col("words").isin(stop_words))

                # Aumenta il numero di partizioni per ottimizzare le prestazioni
                df_words_filtered = df_words_filtered.repartition(100)

                # Conta la frequenza delle parole
                word_counts = df_words_filtered.groupBy("words").count().sort(col("count").desc())

                # Raccogli i risultati e stampali
                word_counts.show(10, truncate=False)

        except Exception as e:
            print(f"Errore nel processare il chunk {i+1}: {e}")

    spark.stop()

def filter_tweets_by_candidate(df, candidate_keywords):
    # Crea una condizione rlike per ogni parola chiave, con un match case-insensitive
    conditions = [col("text").rlike(f"(?i){kw}") for kw in candidate_keywords]

    # Applica la condizione combinando le condizioni con "or"
    combined_condition = conditions.pop(0)
    for condition in conditions:
        combined_condition |= condition

    # Filtra i tweet che soddisfano una delle condizioni
    filtered_df = df.filter(combined_condition)

    # Se non ci sono tweet dopo il filtro, stampa un messaggio e passa al prossimo chunk
    if filtered_df.count() == 0:
        print("Nessun tweet trovato per i candidati nel chunk. Passando al prossimo chunk.")
    else:
        print(f"Numero di tweet dopo il filtro: {filtered_df.count()}")

    return filtered_df

# Esempio di utilizzo
input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"

