from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Inizializza l'analizzatore di sentiment VADER
analyzer = SentimentIntensityAnalyzer()

# Funzione che calcola il sentiment score per ciascun tweet
def get_sentiment_score(text):
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']  # Restituisce il punteggio complessivo di sentiment

# Funzione per eseguire l'analisi del sentiment
def apply_sentiment_analysis(df):
    # Registra la funzione come UDF di Spark
    get_sentiment_udf = udf(get_sentiment_score, FloatType())

    # Aggiungi la colonna 'sentiment_score' al DataFrame
    df_with_sentiment = df.withColumn("sentiment_score", get_sentiment_udf(df["text"]))

    return df_with_sentiment

# Funzione per esportare i dati con il sentiment
def export_with_sentiment(df, output_path):
    df.write.parquet(output_path)

