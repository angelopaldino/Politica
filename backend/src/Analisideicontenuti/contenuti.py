from pyspark.sql.functions import udf, col, split, explode
from pyspark.sql.types import StringType
import os
import streamlit as st
from nltk.corpus import stopwords
import re

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

def count_words_for_candidate(input_path, candidate_keywords):
    if "spark" not in st.session_state:
        raise RuntimeError("Errore: SparkSession non è attiva. Premi 'Avvia App' per iniziarla.")
    else:
        spark = st.session_state.spark

    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        spark.stop()
        return None

    input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.parquet')]

    try:
        df = spark.read.parquet(*input_files)

        if "text" not in df.columns:
            print(f"Errore: La colonna 'text' non è presente nei dati")
            spark.stop()
            return None

        df_filtered = filter_tweets_by_candidate(df, candidate_keywords)

        if df_filtered.count() > 0:
            df_words = df_filtered.withColumn("cleaned_text", clean_text_udf(col("text")))
            df_words = df_words.withColumn("words", explode(split(col("cleaned_text"), " ")))
            df_words_filtered = df_words.filter(~col("words").isin(stop_words))
            df_words_filtered = df_words_filtered.repartition(100)
            word_counts = df_words_filtered.groupBy("words").count().sort(col("count").desc())

            word_counts.show(10, truncate=False)  # Mostra i primi 10 risultati in console

            # Converti il DataFrame Spark in Pandas per il frontend
            return word_counts.toPandas()

        else:
            print("Nessun tweet trovato dopo il filtro per i candidati")
            return None

    except Exception as e:
        print(f"Errore nel caricare o processare i dati: {e}")
        return None


def filter_tweets_by_candidate(df, candidate_keywords):
    # Crea una condizione rlike per ogni parola chiave, con un match case-insensitive
    conditions = [col("text").rlike(f"(?i){kw}") for kw in candidate_keywords]

    # Applica la condizione combinando le condizioni con "or"
    combined_condition = conditions.pop(0)
    for condition in conditions:
        combined_condition |= condition

    # Filtra i tweet che soddisfano una delle condizioni
    filtered_df = df.filter(combined_condition)

    # Se non ci sono tweet dopo il filtro, stampa un messaggio
    if filtered_df.count() == 0:
        print("Nessun tweet trovato per i candidati.")
    else:
        print(f"Numero di tweet dopo il filtro: {filtered_df.count()}")

    return filtered_df
