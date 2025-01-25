from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def pulisci_dati(df: DataFrame) -> DataFrame:

    df = df.dropDuplicates(["tweet_id"])

    colonne_critiche = ["tweet_id", "created_at"]
    df = df.dropna(subset=colonne_critiche)

    df = df.filter(col("created_at").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"))

    df = df.fillna({
        "retweet_count": 0,
        "favorite_count": 0,
        "text": "",
        "hashtags": "",
        "location": "",
        "place_name": "",
        "place_lat": 0.0,
        "place_lon": 0.0
    })

    df = df.filter(~col("text").contains("NA"))
    return df
