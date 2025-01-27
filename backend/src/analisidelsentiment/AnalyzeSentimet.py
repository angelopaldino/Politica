from pyspark.sql import SparkSession, functions as F
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import math
import os

# Funzione per analizzare il sentiment
def analyze_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    score = analyzer.polarity_scores(text)
    return score['compound']

# Funzione per filtrare i tweet per candidato
def filter_tweets_by_candidate(df, candidate_keywords):
    conditions = [F.lower(F.col("text")).contains(kw.lower()) for kw in candidate_keywords]
    combined_condition = conditions.pop(0)
    for condition in conditions:
        combined_condition |= condition
    return df.filter(combined_condition)

# Funzione per elaborare i dati in chunk
def process_in_chunks(input_path, output_path, candidate_keywords):
    spark = SparkSession.builder.appName("BigDataProject").getOrCreate()

    # Verifica l'esistenza del percorso di input
    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        return None

    input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.parquet')]
    total_files = len(input_files)
    chunk_size_percent = 2
    chunk_size = math.ceil(total_files * (chunk_size_percent / 100.0))  # Dimensione del chunk
    num_chunks = math.ceil(total_files / chunk_size)

    # Esegui l'analisi per ogni chunk
    sentiment_results = {'positive': 0, 'neutral': 0, 'negative': 0}
    total_tweets = 0

    for i in range(num_chunks):
        # Estrai i file del chunk corrente
        chunk_files = input_files[i * chunk_size: min((i + 1) * chunk_size, total_files)]
        print(f"Processando chunk {i+1}/{num_chunks}")

        try:
            # Leggi i file del chunk
            df_chunk = spark.read.parquet(*chunk_files)

            # Filtro i tweet per il candidato
            df_filtered = filter_tweets_by_candidate(df_chunk, candidate_keywords)

            # Aggiungi colonna di sentiment
            sentiment_udf = F.udf(analyze_sentiment, returnType=F.DoubleType())
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

        except Exception as e:
            print(f"Errore nel processare il chunk {i+1}: {e}")

    # Calcola le percentuali di sentiment
    sentiment_percentages = {
        'positive': (sentiment_results['positive'] / total_tweets) * 100,
        'neutral': (sentiment_results['neutral'] / total_tweets) * 100,
        'negative': (sentiment_results['negative'] / total_tweets) * 100
    }

    return sentiment_percentages

# Esegui il codice per un candidato
candidate_keywords = ['trump', 'donald trump']
input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"
output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\SentimentAnalysis"
result = process_in_chunks(input_path, output_path, candidate_keywords)
print(result)
