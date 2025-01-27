from Politica.backend.src.Analisidegliutenti.frequenzatweetutente import analyze_user_activity
from Politica.backend.src.Analisitemporale.get_data_for_date import get_data_for_day
from Politica.backend.src.data_io.visualizzazioneparquet import visualizzazioneParquet



def main():
    #print("Backend")
    #CHUNK STYLE ########################
    #get_data_for_day(input_directory,outputDays, "2020-10-10")
    analyze_user_activity(input_directory, outputDays)


    ###############################

#CARTELLE ANGELO ##############################################################

input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"
outputDays = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

########################################


####

####

if __name__ == "__main__":
    main()





