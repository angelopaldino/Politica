# sentiment_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Inizializza il sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Dizionario dei temi
themes = {
    "Politica": {
        "keywords": ["vote", "election", "president", "congress", "politics", "debate", "campaign", "trump", "biden"],
        "hashtags": ["#trump", "#maga", "#biden", "#votebiden", "#election2020", "#debate"]
    },
    "Salute": {
        "keywords": ["health", "covid", "vaccine", "pandemic", "hospital", "virus"],
        "hashtags": ["#covid19", "#health", "#pandemic", "#vaccine"]
    },
    "Sociale": {
        "keywords": ["rights", "justice", "equality", "protest", "democracy", "freedom"],
        "hashtags": ["#blacklivesmatter", "#BLM", "#metoo", "#socialjustice"]
    },
    "Economia": {
        "keywords": ["economy", "growth", "recession", "stock", "market", "jobs", "unemployment"],
        "hashtags": ["#economy", "#stocks", "#growth", "#recession"]
    },
    "Ambiente": {
        "keywords": ["climate", "earth", "sustainability", "green", "environment", "pollution", "renewable"],
        "hashtags": ["#climatechange", "#greenenergy", "#earth", "#sustainability"]
    },
    "Tecnologia": {
        "keywords": ["tech", "innovation", "ai", "artificialintelligence", "blockchain", "5G", "startups", "technews"],
        "hashtags": ["#tech", "#AI", "#blockchain", "#5G", "#startups"]
    },
    "Sport": {
        "keywords": ["football", "soccer", "Olympics", "sport", "athlete", "game", "competition"],
        "hashtags": ["#football", "#soccer", "#olympics", "#sports"]
    }
}

# Funzione di analisi del sentiment (positività o negatività)
def analyze_sentiment(text):
    sentiment_score = analyzer.polarity_scores(text)["compound"]
    if sentiment_score >= 0.05:
        return "Positivo"
    elif sentiment_score <= -0.05:
        return "Negativo"
    else:
        return "Neutro"

# Funzione di classificazione del tema in base al testo e agli hashtag
def classify_tweet(text, hashtags):
    for theme, data in themes.items():
        # Controlla se gli hashtag sono associati al tema
        if any(hashtag in hashtags for hashtag in data["hashtags"]):
            return theme

        # Controlla se il testo contiene parole chiave associate al tema
        if any(keyword in text.lower() for keyword in data["keywords"]):
            return theme

    return "Generico"  # Se non c'è corrispondenza, restituisci "Generico"

# Crea un UDF (User Defined Function) per applicare la classificazione al DataFrame
classify_tweet_udf = udf(classify_tweet, StringType())

# Funzione principale per la creazione e salvataggio dei DataFrame suddivisi per tema
def process_tweets(input_path, output_dir):
    # Inizializza SparkSession
    spark = SparkSession.builder \
        .appName("TwitterThemeClassification") \
        .master("local[*]") \
        .getOrCreate()

    # Carica il dataset Parquet
    df = spark.read.parquet(input_path)

    # Aggiungi la colonna 'theme' che rappresenta il tema del tweet
    df = df.withColumn("theme", classify_tweet_udf(df["text"], df["hashtags"]))

    # Suddividi il DataFrame in base al tema
    themes_df = {}
    for theme in themes.keys():
        themes_df[theme] = df.filter(df["theme"] == theme)

    # Salva i DataFrame suddivisi in formato Parquet
    for theme, theme_df in themes_df.items():
        theme_df.write.parquet(f"{output_dir}/{theme.lower()}_tweets.parquet", mode="overwrite")

    # Fermati la sessione di Spark
    spark.stop()

