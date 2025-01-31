import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd

from Politica.backend.src.analisidelsentiment.all_candidatesSentiment import get_sentiment_for_candidates


# Funzione per visualizzare il grafico del sentiment
def show_sentiment_graph():
    st.title("Analisi del Sentiment per Candidati")

    # Lista dei candidati
    candidates = [
        "Joe Biden", "Donald Trump", "Bernie Sanders", "Elizabeth Warren", "Kamala Harris",
        "Pete Buttigieg", "Cory Booker", "Andrew Yang", "Beto O’Rourke"
    ]

    # Percorso dei dati
    data_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"

    # Esegui l'analisi del sentiment per i candidati
    sentiment_data = get_sentiment_for_candidates(data_path, candidates)

    if sentiment_data is None:
        st.error("Errore durante l'analisi del sentiment.")
        return

    # Creazione del DataFrame per il grafico
    sentiment_df = pd.DataFrame(sentiment_data).T  # Trasponiamo il dizionario per avere i candidati come righe

    # Visualizza i dati
    st.write(sentiment_df)

    # Creazione del grafico a barre
    fig, ax = plt.subplots(figsize=(10, 6))
    sentiment_df['positive'].plot(kind='barh', color='green', ax=ax, label="Positivo")
    sentiment_df['negative'].plot(kind='barh', color='red', ax=ax, label="Negativo")
    sentiment_df['neutral'].plot(kind='barh', color='gray', ax=ax, label="Neutrale")

    ax.set_xlabel("Percentuale di Sentiment (%)")
    ax.set_title("Sentiment dei Candidati per il mese di Ottobre")
    ax.legend()

    st.pyplot(fig)