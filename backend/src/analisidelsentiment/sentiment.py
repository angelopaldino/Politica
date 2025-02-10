from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
import os
import streamlit as st
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer



analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    if not text:
        return 0.0

    score = analyzer.polarity_scores(text)
    return score.get('compound', 0.0)


def filter_tweets_by_candidate(df, candidate_keywords):
    conditions = [F.lower(F.col("text")).contains(kw.lower()) for kw in candidate_keywords]
    combined_condition = conditions.pop(0)
    for condition in conditions:
        combined_condition |= condition
    return df.filter(combined_condition)


def process_data2(input_path, candidate_keywords):
    if "spark" not in st.session_state:
        raise RuntimeError("Errore: SparkSession non è attiva. Premi 'Avvia App' per iniziarla.")
    else:
        spark = st.session_state.spark


    if not os.path.exists(input_path):
        print(f"Errore: Il percorso di input non esiste: {input_path}")
        return None


    df = spark.read.parquet(input_path)


    df_filtered = filter_tweets_by_candidate(df, candidate_keywords)

    if df_filtered.count() == 0:
        print(f"Nessun tweet trovato per il candidato.")
        return None


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


    sentiment_results = {'positive': 0, 'neutral': 0, 'negative': 0}
    total_tweets = df_filtered.count()

    for row in sentiment_counts.collect():
        sentiment_results[row['sentiment_label']] = row['count']


    sentiment_percentages = {
        'positive': (sentiment_results['positive'] / total_tweets) * 100,
        'neutral': (sentiment_results['neutral'] / total_tweets) * 100,
        'negative': (sentiment_results['negative'] / total_tweets) * 100
    }

    return sentiment_percentages
