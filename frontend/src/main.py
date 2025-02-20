import streamlit as st
from Politica.backend.app import stop_spark_session, get_spark_session
from Politica.frontend.src.Analisidegliutenti.activity import user_activity2
from Politica.frontend.src.Analisidegliutenti.generals import generals
from Politica.frontend.src.Analisideicontenuti.counts import Tweets2
from Politica.frontend.src.Analisideicontenuti.tema import analyze_hashtags, themes_keywords2, plot_pie_chart
from Politica.frontend.src.Analisidelsentiment.all_candidates_sentiment import show_sentiment_graph
from Politica.frontend.src.Analisidelsentiment.sentiment import analisi2
from Politica.frontend.src.analisitemporale.days import getDays2
import pandas as pd

from Politica.frontend.src.analisitemporale.temporal_tweets import show_tweet_analysis
from Politica.frontend.src.geo.fasce import fasce
from Politica.frontend.src.geo.geolocalizzazione import show_map


# Avvio della SparkSession se non è già attiva
def start_spark():
    if "spark" not in st.session_state:
        st.session_state.spark = get_spark_session()
        st.session_state.spark_active = True

# Arresto della SparkSession
def stop_spark():
    stop_spark_session()
    if "spark" in st.session_state:
        del st.session_state.spark
    st.session_state.spark_active = False

# UI principale
st.title("Applicazione BigData Politica")

# Se Spark non è attiva, mostra "Avvia App". Se è attiva, mostra "Stop App".
if not st.session_state.get("spark_active", False):
    if st.button("Avvia App"):
        start_spark()
        st.experimental_rerun()  # Ricarica la pagina per aggiornare l'UI
else:
    if st.button("Stop App"):
        stop_spark()
        st.experimental_rerun()  # Ricarica la pagina per aggiornare l'UI
    st.success("SparkSession attiva!")

# only debug
#if st.session_state.get("spark_active", False):
 #   df = st.session_state.spark.createDataFrame([(1, "test")], ["id", "name"])
  #  st.dataframe(df.toPandas())  # Usa toPandas() per mostrare i dati in Streamlit

# Funzione per la home con la classifica
def home():
    st.title("Benvenuto nell'app per le Elezioni Politiche in America 2k20")
    st.write("Seleziona una funzionalità dal menu a sinistra oppure esplora i 10 hashtag più popolari del mese!")

    # Classifica hashtag
    hashtags = [
        ("#vote", 826082), ("#realdonaldtrump", 461302), ("#1", 362900),
        ("#trump", 333436), ("#covid19", 306124), ("#maga", 296860),
        ("#trump2020", 239244), ("#debates2020", 212686), ("#bidenharris2020", 206276),
        ("#joebiden", 200624),
    ]

    st.subheader("Top 10 Hashtags di ottobre:")
    for i, (hashtag, count) in enumerate(hashtags, start=1):
        st.write(f"**{i}. {hashtag}** - {count}")

    # Grafico a barre
    st.bar_chart(pd.DataFrame(hashtags, columns=["Hashtag", "Count"]).set_index("Hashtag"))

    # Solo quando Spark è attiva, mostra il grafico a torta
    if st.session_state.get("spark_active", False):
        st.subheader("Distribuzione Temi degli Hashtags")
        file_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Big Data\\output\\hashtags_output.txt"
        theme_counts = analyze_hashtags(file_path, themes_keywords2)
        plot_pie_chart(theme_counts)

# Menu di navigazione
def main():
    menu = st.sidebar.selectbox(
        "Scegli la funzionalità",
        ["Home", "Analisi Tweets", "Analisi generali", "Analisi lingue e posizione più comune",  "Analisi Sentiment", "Tweets del giorno", "Analisi Attività Utente", "Mappa geografica dei Tweets", "Analisi del Numero di Tweet per Periodo", "Guarda chi sta vincendo"]
    )

    # Verifica se Spark è attiva prima di eseguire le funzioni
    if not st.session_state.get("spark_active", False):
        st.write("Devi avviare l'applicazione per poter accedere alle funzionalità!")
        return

    if menu == "Home":
        home()
    elif menu == "Analisi Tweets":
        Tweets2()
    elif menu == "Mappa geografica dei Tweets":
        if st.button("Genera Mappa"):
            show_map()
    elif menu == "Analisi Sentiment":
        analisi2()
    elif menu == "Analisi lingue e posizione più comune":
        fasce()
    elif menu == "Tweets del giorno":
        getDays2()
    elif menu == "Analisi Attività Utente":
        user_activity2()
    elif menu == "Analisi del Numero di Tweet per Periodo":
        show_tweet_analysis()
    elif menu == "Guarda chi sta vincendo":
        show_sentiment_graph()
    elif menu == "Analisi generali":
        generals()

if __name__ == "__main__":
    main()
