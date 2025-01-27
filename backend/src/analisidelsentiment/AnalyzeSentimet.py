from pyspark.sql import SparkSession, functions as F
import math
import os
from pyspark.sql.types import DoubleType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

os.environ['PYSPARK_PYTHON'] = 'C:/Users/angel/AppData/Local/Programs/Python/Python310/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/angel/AppData/Local/Programs/Python/Python310/python.exe'

analyzer = SentimentIntensityAnalyzer()

# Funzione per analizzare il sentiment
def analyze_sentiment(text):
    if not text:
        return 0.0  # Restituisce un valore predefinito per input nulli

    score = analyzer.polarity_scores(text)
    return score.get('compound', 0.0)  # Restituisce 0.0 se 'compound' non esiste

# Funzione per filtrare i tweet per candidato
def filter_tweets_by_candidate(df, candidate_keywords):
    conditions = [F.lower(F.col("text")).contains(kw.lower()) for kw in candidate_keywords]
    combined_condition = conditions.pop(0)
    for condition in conditions:
        combined_condition |= condition
    return df.filter(combined_condition)

# Funzione per elaborare i dati in chunk
def process_in_chunks(input_path, output_path, candidate_keywords):
    spark = SparkSession.builder \
        .appName("Analisi del sentiment") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()

    # Verifica l'esistenza del percorso di input
    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        return None

    input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.parquet')]
    total_files = len(input_files)
    chunk_size_percent = 1
    chunk_size = math.ceil(total_files * (chunk_size_percent / 100.0))  # Dimensione del chunk
    num_chunks = math.ceil(total_files / chunk_size)

    # Esegui l'analisi per ogni chunk
    sentiment_results = {'positive': 0, 'neutral': 0, 'negative': 0}
    total_tweets = 0
    tweet_counter = 0  # Contatore dei tweet elaborati per log

    for i in range(num_chunks):
        # Estrai i file del chunk corrente
        chunk_files = input_files[i * chunk_size: min((i + 1) * chunk_size, total_files)]
        print(f"Processando chunk {i+1}/{num_chunks}")

        try:
            # Leggi i file del chunk
            df_chunk = spark.read.parquet(*chunk_files)

            # Numero di tweet nel chunk prima del filtro
            print(f"Numero di tweet nel chunk {i+1} prima del filtro: {df_chunk.count()}")

            # Filtro i tweet per il candidato
            df_filtered = filter_tweets_by_candidate(df_chunk, candidate_keywords)
            print(f"Numero di tweet nel chunk {i+1} dopo il filtro: {df_filtered.count()}")

            if df_filtered.count() == 0:
                print(f"Chunk {i+1}: Nessun tweet trovato per il candidato.")
                continue

            # Aggiungi colonna di sentiment
            sentiment_udf = F.udf(analyze_sentiment, returnType=DoubleType())
            df_filtered = df_filtered.withColumn('sentiment', sentiment_udf(F.col('text')))

            # Classifica i sentimenti
            df_filtered = df_filtered.withColumn(
                'sentiment_label',
                F.when(F.col('sentiment') > 0.05, 'positive')
                .when(F.col('sentiment') < -0.05, 'negative')
                .otherwise('neutral')
            )

            # Calcola il numero di sentimenti
            sentiment_counts = df_filtered.groupBy('sentiment_label').count()
            total_tweets += df_filtered.count()

            # Aggiorna i conteggi dei sentimenti
            for row in sentiment_counts.collect():
                sentiment_results[row['sentiment_label']] += row['count']

            # Aggiorna il contatore dei tweet elaborati e stampa il progresso ogni 100.000 tweet
            tweet_counter += df_filtered.count()
            if tweet_counter >= 100000:
                print(f"Progressivo: {tweet_counter} tweet elaborati fino ad ora.")
                tweet_counter = 0  # Reset del contatore ogni volta che raggiunge il target

        except Exception as e:
            print(f"Errore nel processare il chunk {i+1}: {e}")

    # Calcola le percentuali di sentiment
    sentiment_percentages = {
        'positive': (sentiment_results['positive'] / total_tweets) * 100,
        'neutral': (sentiment_results['neutral'] / total_tweets) * 100,
        'negative': (sentiment_results['negative'] / total_tweets) * 100
    }

    return sentiment_percentages



