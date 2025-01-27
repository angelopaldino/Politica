import streamlit as st
import matplotlib.pyplot as plt
from collections import defaultdict

from Politica.frontend.src.Analisideicontenuti.temaPerHashtags import analyze_hashtags, themes_keywords


# Funzione per la home con il banner della classifica
def home():
    st.title("Benvenuto nell'app per le Elezioni Politiche in America 2k20")

    # Testo introduttivo
    st.write("Seleziona una funzionalità dal menu a sinistra oppure esplora i 10 hashtag più popolari del mese!")

    # Esempio: classifica dei 10 hashtag più usati
    hashtags = [
        ("#vote", 826082),
        ("#realdonaldtrump", 461302),
        ("#1", 362900),
        ("#trump", 333436),
        ("#covid19", 306124),
        ("#maga", 296860),
        ("#trump2020", 239244),
        ("#debates2020", 212686),
        ("#bidenharris2020", 206276),
        ("#joebiden", 200624),
    ]

    # Visualizza un banner con la classifica
    st.subheader("Top 10 Hashtags di ottobre:")
    for i, (hashtag, count) in enumerate(hashtags, start=1):
        st.write(f"**{i}. {hashtag}** - {count} ")

    # Opzionale: aggiungere un grafico per migliorare la visualizzazione
    st.bar_chart([count for _, count in hashtags])

    # Inserire il grafico a torta per i temi
    st.subheader("Distribuzione Temi degli Hashtags")
    # Esegui l'analisi degli hashtag (assicurati che il file con il percorso corretto sia presente)
    file_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Big Data\\output\\hashtags_output.txt"
    theme_counts = analyze_hashtags(file_path, themes_keywords)

    # Visualizza il grafico a torta
    plot_pie_chart(theme_counts)

# Import delle altre funzioni
from Politica.frontend.src.Analisideicontenuti.CountWords import Tweets
from Politica.frontend.src.Analisidelsentiment.sentimentAnalysis import analisi, plot_pie_chart
from Politica.frontend.src.analisitemporale.getDays import getDays

# Main Streamlit
def main():
    # Menu di navigazione nel sidebar
    menu = st.sidebar.selectbox(
        "Scegli la funzionalità",
        ["Home", "Analisi Tweets", "Analisi Sentiment", "Tweets del giorno"]
    )

    # Logica per caricare la pagina selezionata
    if menu == "Home":
        home()  # Chiama la funzione della Home con la classifica

    elif menu == "Analisi Tweets":
        Tweets()  # Chiama la funzione corrispondente nel file `tweets.py`

    elif menu == "Analisi Sentiment":
        analisi()  # Chiama la funzione corrispondente nel file `sentiment.py`

    elif menu == "Tweets del giorno":
        getDays()  # Chiama la funzione corrispondente nel file `getDays.py`

if __name__ == "__main__":
    main()

