from pyspark.sql import SparkSession
from src.analysis.temporalDay_analysis import suddividi_per_giorno
from src.analysis.temporalHours import suddividi_per_fascia_oraria
from src.analysis.temporalWeek_analysis import suddividi_per_settimana
from src.data_io.loader import testoEHashtags
from src.sentiment_analysis import process_tweets
from src.visualizzazioneparquet import visualizzazioneParquet, visualizzazioneDay


def main():
    #PRIMA DI LANCIARE LE FUNZIONI DI SUDDIVISIONI COMMENTARE LO SPARKSESSIONBUILDER DEL MAIN DI QUESTOFILE
    #PERCHE' NEGLI SCRIPT DELLE SUDDIVISIONI VIENE CREATI LI IL SESSION BUILDER SPARK

    #visualizzazioneParquet()
    #visualizzazioneDay(input_pathDay)
    #suddividi_per_giorno(input_directory, outputDays) #TODO RUN
    #suddividi_per_settimana(input_directory, outputWeeks) #TODO RUN
    #suddividi_per_fascia_oraria(input_directory, outputHours) #TODO RUN

    ##analisi del sentiment
    #PASSO 1 CREARE UN DATASET CON TESTO E HASHTAGS
    #testoEHashtags(input_directory,outputTH)  #TODO RUN
    #PASSO 2 LANCIARE QUESTA
    #process_tweets(outputTH, sentiment_dir) #TODO RUN




#CARTELLE ANGELO ##############################################################
outputTH = "C:\\Users\\angel\\OneDrive\\Desktop\\TestoEHashtags"
sentiment_dir = "C:\\Users\\angel\\OneDrive\\Desktop\\SentimentAnalysis"
root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"
input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset"
input_pathDay = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"
output_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\BigDataPolitica\\Politica\\data\\processed\\pulito"
outputDays = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"
outputWeeks = "C:\\Users\\angel\\OneDrive\\Desktop\\WeeksAnalysis"
outputHours = "C:\\Users\\angel\\OneDrive\\Desktop\\HoursAnalysis"
########################################


####

####

if __name__ == "__main__":
    main()





