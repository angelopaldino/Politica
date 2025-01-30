from pyspark.sql import SparkSession
from Politica.backend.src.Analisidegliutenti.frequenzatweetutente import analyze_user_activity
from Politica.backend.src.Analisitemporale.get_data_for_date import get_data_for_day
from Politica.backend.src.data_io.visualizzazioneparquet import visualizzazioneParquet



def main():
    print("Backend")
    #CHUNK STYLE ########################
    #get_data_for_day(input_sub,output, "2020-10-10")
    #analyze_user_activity(input_sub, output)


    ###############################

#CARTELLE ANGELO ##############################################################
input_sub = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"
input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"
output = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

########################################


####

####

if __name__ == "__main__":
    main()





