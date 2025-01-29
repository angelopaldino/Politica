import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd

from Politica.backend.src.analisidelsentiment.AnalyzeSentimet import process_in_chunks

# Lista dei candidati
candidates = [
    "Joe Biden", "Donald Trump", "Bernie Sanders", "Elizabeth Warren", "Kamala Harris", "Pete Buttigieg"
, 'Cory Booker','Andrew Yang','Beto O’Rourke','']

# Funzione per visualizzare il grafico a torta
def plot_pie_chart(sentiment_percentages):
    labels = sentiment_percentages.keys()
    sizes = sentiment_percentages.values()

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['green', 'red', 'gray'])
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    st.pyplot(fig)

def analisi():
    # Interfaccia utente
    st.title("Analisi del Sentiment sui Tweet")
    st.write("Scegli un candidato per eseguire l'analisi del sentiment sui tweet.")

    # Menu a tendina per scegliere il candidato
    selected_candidate = st.selectbox("Seleziona un candidato", candidates)

    # Percorsi dei dati
    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"
    output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\SentimentAnalysis"

    if st.button("Esegui Analisi"):
        st.write(f"Analizzando i tweet per {selected_candidate}...")

        # Modifica il testo per adattarlo ai formati variabili (es. "Joe Biden" o "biden")
        candidate_keywords = [selected_candidate.lower(), selected_candidate.split()[1].lower()]

        # Esegui l'analisi del sentiment
        sentiment_percentages = process_in_chunks(input_path, output_path, candidate_keywords)

        if sentiment_percentages:
            st.write(f"Percentuali di sentiment per {selected_candidate}:")
            st.write(sentiment_percentages)

            # Visualizza il grafico
            plot_pie_chart(sentiment_percentages)
        else:
            st.write("Nessun dato disponibile per l'analisi.")

