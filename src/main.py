from pyspark.sql import SparkSession
import pyarrow
import grpc

from src.analysis.sentiment_analysis import apply_sentiment_analysis, export_with_sentiment
from src.analysis.temporalDay_analysis import suddividi_per_giorno
from src.analysis.temporalHours import suddividi_per_fascia_oraria
from src.analysis.temporalWeek_analysis import suddividi_per_settimana
from src.visualizzazioneparquet import visualizzazioneParquet, visualizzazioneDay


def main():
    spark = SparkSession.builder \
        .appName("SentimentAnalysis") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    df = spark.read.parquet(input_directory)
    df_with_sentiment = apply_sentiment_analysis(df)

    positive_tweets = df_with_sentiment.filter(df_with_sentiment["sentiment_score"] > 0)
    negative_tweets = df_with_sentiment.filter(df_with_sentiment["sentiment_score"] < 0)
    neutral_tweets = df_with_sentiment.filter(df_with_sentiment["sentiment_score"] == 0)

    export_with_sentiment(df_with_sentiment, sentiment_dir)

    #PRIMA DI LANCIARE LE FUNZIONI DI SUDDIVISIONI COMMENTARE LO SPARKSESSIONBUILDER DEL MAIN DI QUESTOFILE
    #PERCHE' NEGLI SCRIPT DELLE SUDDIVISIONI VIENE CREATI LI IL SESSION BUILDER SPARK

    #visualizzazioneParquet()
    #visualizzazioneDay(input_pathDay)
    #suddividi_per_giorno(input_directory, outputDays) #TODO RUN
    #suddividi_per_settimana(input_directory, outputWeeks) #TODO RUN
    #suddividi_per_fascia_oraria(input_directory, outputHours) #TODO RUN




#CARTELLE ##############################################################
sentiment_dir = "C:\\Users\\angel\\OneDrive\\Desktop\\SentimentAnalysis"
root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"
input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset"
input_pathDay = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"
output_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\BigDataPolitica\\Politica\\data\\processed\\pulito"
outputDays = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"
outputWeeks = "C:\\Users\\angel\\OneDrive\\Desktop\\WeeksAnalysis"
outputHours = "C:\\Users\\angel\\OneDrive\\Desktop\\HoursAnalysis"
########################################

if __name__ == "__main__":
    main()





