from Politica.backend.src.Analisitemporale.get_data_for_date import get_data_for_day
from Politica.backend.src.data_io.visualizzazioneparquet import visualizzazioneParquet



def main():
    visualizzazioneParquet()
    #visualizzazioneDay(input_pathDay)
    #suddividi_per_giorno(input_directory, outputDays) #TODO RUN
    #suddividi_per_settimana(input_directory, outputWeeks) #TODO RUN
    #suddividi_per_fascia_oraria(input_directory, outputHours) #TODO RUN
    #subset(input_directory,outputSubset )

    #CHUNK STYLE ########################
    #get_data_for_day(input_directory,outputDays, target_data)


    ###############################




    ##analisi del sentiment
    #PASSO 1 CREARE UN DATASET CON TESTO E HASHTAGS
    #testoEHashtags(input_directory,outputTH)  #TODO RUN
    #PASSO 2 LANCIARE QUESTA
    #process_tweets(outputTH, sentiment_dir) #TODO RUN




#CARTELLE ANGELO ##############################################################

input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset"
outputDays = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

########################################


####

####

if __name__ == "__main__":
    main()





