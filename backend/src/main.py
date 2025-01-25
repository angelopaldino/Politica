from backend.src.Analisitemporale.get_data_for_date import get_data_for_day


def main():
    #visualizzazioneParquet()
    #visualizzazioneDay(input_pathDay)
    #suddividi_per_giorno(input_directory, outputDays) #TODO RUN
    #suddividi_per_settimana(input_directory, outputWeeks) #TODO RUN
    #suddividi_per_fascia_oraria(input_directory, outputHours) #TODO RUN
    #subset(input_directory,outputSubset )

    #CHUNK STYLE ########################

    get_data_for_day(input_directory,outputDays, "2020-10-15")


    ###############################




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
outputSubset = "C:\\Users\\angel\\OneDrive\\Desktop\\SubsetParquet"
########################################


####

####

if __name__ == "__main__":
    main()





