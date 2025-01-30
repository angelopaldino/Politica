import streamlit as st
from Politica.backend.app import get_spark_session, stop_spark_session
from Politica.frontend.src.Analisideicontenuti.CountWords import Tweets
from Politica.frontend.src.Analisidelsentiment.sentimentAnalysis import analisi, plot_pie_chart
from Politica.frontend.src.analisitemporale.getDays import getDays
from Politica.frontend.src.Analisidegliutenti.user_activity import user_activity
from Politica.frontend.src.Analisideicontenuti.temaPerHashtags import analyze_hashtags, themes_keywords
import pandas as pd

# Avvio della SparkSession se non è già attiva
def ensure_spark_session():
    if "spark" not in st.session_state:
        st.session_state.spark = get_spark_session()

# Funzione per la home
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

    # Grafico a torta
    st.subheader("Distribuzione Temi degli Hashtags")
    file_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Big Data\\output\\hashtags_output.txt"
    theme_counts = analyze_hashtags(file_path, themes_keywords)
    plot_pie_chart(theme_counts)

# UI principale
st.title("Applicazione BigDataPolitica")

# Pulsanti per avviare e fermare SparkSession
col1, col2 = st.columns(2)
with col1:
    if st.button("Avvia App"):
        ensure_spark_session()
        st.success("SparkSession avviata!")

with col2:
    if st.button("Stop App"):
        stop_spark_session()
        if "spark" in st.session_state:
            del st.session_state.spark
        st.warning("SparkSession terminata!")

# Esempio di query
ensure_spark_session()  # Garantisce che Spark sia avviato prima di eseguire qualsiasi query
if "spark" in st.session_state:
    df = st.session_state.spark.createDataFrame([(1, "test")], ["id", "name"])
    st.dataframe(df.toPandas())  # Usa toPandas() per mostrare i dati in Streamlit

# Main Streamlit
def main():
    menu = st.sidebar.selectbox(
        "Scegli la funzionalità",
        ["Home", "Analisi Tweets", "Analisi Sentiment", "Tweets del giorno", "Analisi Attività Utente"]
    )

    # Controllo Spark prima di eseguire una funzionalità
    ensure_spark_session()

    if menu == "Home":
        if "spark" not in st.session_state:
            st.error("Devi avviare l'app prima di eseguire una query!")
        else:
            home()
    elif menu == "Analisi Tweets":
        if "spark" not in st.session_state:
            st.error("Devi avviare l'app prima di eseguire una query!")
        else:
            Tweets()
    elif menu == "Analisi Sentiment":
        if "spark" not in st.session_state:
            st.error("Devi avviare l'app prima di eseguire una query!")
        else:
            analisi()
    elif menu == "Tweets del giorno":
        if "spark" not in st.session_state:
            st.error("Devi avviare l'app prima di eseguire una query!")
        else:
            getDays()
    elif menu == "Analisi Attività Utente":
        if "spark" not in st.session_state:
            st.error("Devi avviare l'app prima di eseguire una query!")
        else:
            user_activity()

if __name__ == "__main__":
    main()
