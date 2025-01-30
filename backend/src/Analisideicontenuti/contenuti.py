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

    # Verifica che il percorso di input esista
    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        spark.stop()
        return None

    # Ottieni la lista dei file Parquet nel percorso di input
    input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.parquet')]

    try:
        # Carica tutti i file Parquet in un DataFrame
        df = spark.read.parquet(*input_files)

        # Verifica la presenza della colonna 'text'
        if "text" not in df.columns:
            print(f"Errore: La colonna 'text' non è presente nei dati")
            spark.stop()
            return None

        # Estrai e pulisci il testo, poi filtra i tweet che menzionano i candidati
        df_filtered = filter_tweets_by_candidate(df, candidate_keywords)

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

        else:
            print("Nessun tweet trovato dopo il filtro per i candidati")

    except Exception as e:
        print(f"Errore nel caricare o processare i dati: {e}")

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

    # Se non ci sono tweet dopo il filtro, stampa un messaggio
    if filtered_df.count() == 0:
        print("Nessun tweet trovato per i candidati.")
    else:
        print(f"Numero di tweet dopo il filtro: {filtered_df.count()}")

    return filtered_df
