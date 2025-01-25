from pyspark.sql import SparkSession
import pyarrow
import grpc
from src.analysis.temporalDay_analysis import suddividi_per_giorno
from src.analysis.temporalHours import suddividi_per_fascia_oraria
from src.analysis.temporalWeek_analysis import suddividi_per_settimana
from src.visualizzazioneparquet import visualizzazioneParquet, visualizzazioneDay


def main():
    #visualizzazioneParquet()
    #visualizzazioneDay(input_pathDay)
    suddividi_per_giorno(input_directory, outputDays)
    #suddividi_per_settimana(input_directory, outputWeeks) #TODO RUN
    #suddividi_per_fascia_oraria(input_directory, outputHours) #TODO RUN

root_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\dataset politica\\"
input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset"
input_pathDay = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"
output_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\BigDataPolitica\\Politica\\data\\processed\\pulito"
outputDays = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"
outputWeeks = "C:\\Users\\angel\\OneDrive\\Desktop\\WeeksAnalysis"
outputHours = "C:\\Users\\angel\\OneDrive\\Desktop\\HoursAnalysis"

if __name__ == "__main__":
    main()





