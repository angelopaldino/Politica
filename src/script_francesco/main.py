from pyspark.sql import SparkSession
from wordcount import word_count
from conversioneParquet import multi_conversione



def create_spark_session(app_name=str):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.driver.memory", "14g")
            .config("spark.executor.memory", "14g")
            .getOrCreate())



def leggi_dataset(spark: SparkSession):

    percorso_parquet = "dataset\\"  

    df = spark.read.parquet(percorso_parquet)


    df.printSchema()
    print("row_count: "+ str(df.count())+"\n")
    df.show() 




def main() :
    spark_session = create_spark_session(app_name="WorldCount")

    multi_conversione(spark_session)
    leggi_dataset(spark_session)
    word_count(spark_session)
    
    spark_session.stop()


if __name__ == "__main__":
    main()