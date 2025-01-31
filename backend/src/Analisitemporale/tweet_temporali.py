import os
from pyspark.sql.functions import to_date, weekofyear, month, col
import streamlit as st

def tweet_per_temporalita(data_path):
    if "spark" not in st.session_state:
        raise RuntimeError("Errore: SparkSession non è attiva. Premi 'Avvia App' per iniziarla.")
    else:
        spark = st.session_state.spark

    # Verifica se il percorso esiste
    if not os.path.exists(data_path):
        print(f"Errore: Il percorso di input non esiste: {data_path}")
        return None

    try:
        input_files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith('.parquet')]
        if not input_files:
            print(f"Errore: Nessun file Parquet trovato nel percorso di input: {data_path}")
            return None
    except FileNotFoundError:
        print(f"Errore: Percorso di input non trovato: {data_path}")
        return None

    # Leggi i file Parquet
    df = spark.read.parquet(*input_files)

    # Mostra le prime righe per verificare la presenza della colonna 'created_at'
    df.show(5)

    # Controlla il tipo di dato della colonna 'created_at'
    df.printSchema()

    # Converte la colonna 'created_at' in formato data
    df = df.withColumn('created_at', to_date(col('created_at')))

    # Mostra i primi 5 record dopo la conversione
    df.show(5)

    # Conta i record prima del filtro
    print(f"Numero di record prima del filtro: {df.count()}")

    # Filtra i tweet solo per ottobre (considera solo le date tra 01/10 e 31/10)
    df = df.filter((col('created_at').between('2020-10-01', '2020-10-31')))

    # Conta i record dopo il filtro
    print(f"Numero di record dopo il filtro: {df.count()}")

    # Numero di tweet per giorno
    df_day = df.groupBy('created_at').count().orderBy('created_at')
    df_day.show(5)

    # Numero di tweet per settimana (usando weekofyear)
    df_week = df.withColumn('week', weekofyear(col('created_at'))) \
        .groupBy('week').count().orderBy('week')
    df_week.show(5)

    # Numero di tweet per mese (usando month)
    df_month = df.withColumn('month', month(col('created_at'))) \
        .groupBy('month').count().orderBy('month')
    df_month.show(5)

    # Converti i risultati in Pandas per facilitarne l'uso in Streamlit
    df_day_pandas = df_day.toPandas()
    df_week_pandas = df_week.toPandas()
    df_month_pandas = df_month.toPandas()

    # Verifica i dati Pandas
    print(f"DataFrame per giorno: {df_day_pandas.head()}")
    print(f"DataFrame per settimana: {df_week_pandas.head()}")
    print(f"DataFrame per mese: {df_month_pandas.head()}")

    print(f"Analisi completata. Restituisco {len(df_day_pandas)} record per giorno, {len(df_week_pandas)} per settimana, {len(df_month_pandas)} per mese.")

    return df_day_pandas, df_week_pandas, df_month_pandas


