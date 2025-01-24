import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession

def leggi_csv(spark_session: SparkSession, percorso_file: str):

    return (spark_session.read
        .format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", "true")
        .option("mode", "DROPMALFORMED")
        .load(percorso_file))

def multi_conversione(spark_session: SparkSession, max_workers=16):
    
    cartella_input = "C:\\Users\\franc\\Desktop\\Dataset\\"
    output_base_dir = "dataset\\"
    
    os.makedirs(output_base_dir, exist_ok=True)
    
    file_paths = [os.path.join(cartella_input, f"tweet_USA_{i}_october.csv") for i in range(1, 32)]
    
    all_dataframes = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(leggi_csv, spark_session, file): file for file in file_paths}
        
        for future in as_completed(futures):
            file = futures[future]
            try:
                df = future.result()
                all_dataframes.append(df)
                print(f"Processato con successo: {file}")
            except Exception as exc:
                print(f'Errore nel file {file}: {exc}')
    
    consolidated_df = spark_session.createDataFrame([], schema=all_dataframes[0].schema)
    for df in all_dataframes:
        consolidated_df = consolidated_df.union(df)
    
    consolidated_df.write.mode("overwrite").parquet(os.path.join(output_base_dir, ""))
    
    print("Conversione completata. Dataset Parquet consolidato creato.")