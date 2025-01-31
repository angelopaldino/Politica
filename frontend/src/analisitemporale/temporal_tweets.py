import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from Politica.backend.src.Analisitemporale.tweet_temporali import tweet_per_temporalita
import matplotlib.dates as mdates

def show_tweet_analysis():
    st.title("Analisi del Numero di Tweet per Periodo")

    # Percorso dei dati
    data_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"

    # Avvia l'analisi dei dati temporali
    df_day, df_week, df_month = tweet_per_temporalita(data_path)

    if df_day is None or df_week is None or df_month is None:
        st.error("Errore durante l'analisi dei dati.")
        return

    # Seleziona tra i vari grafici
    analysis_option = st.selectbox("Scegli l'analisi temporale", ["Per Giorno", "Per Settimana", "Per Mese"])

    # Mostra il grafico in base alla selezione
    if analysis_option == "Per Giorno":
        st.subheader("Numero di Tweet per Giorno")
        st.write(df_day)

        # Grafico per giorno
        fig, ax = plt.subplots()
        ax.plot(df_day['created_at'], df_day['count'], marker='o', color='b', label='Tweet per giorno')
        ax.set_xlabel("Giorno")
        ax.set_ylabel("Numero di Tweet")
        ax.set_title("Numero di Tweet per Giorno")

        # Migliora la visualizzazione dell'asse X
        ax.xaxis.set_major_locator(mdates.WeekdayLocator())  # Posizione degli intervalli
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))  # Formato della data
        plt.xticks(rotation=45)  # Ruota le etichette

        ax.grid(True)
        st.pyplot(fig)

    elif analysis_option == "Per Settimana":
        st.subheader("Numero di Tweet per Settimana")
        st.write(df_week)

        # Grafico per settimana
        fig, ax = plt.subplots()
        ax.plot(df_week['week'], df_week['count'], marker='o', color='g', label='Tweet per settimana')
        ax.set_xlabel("Settimana")
        ax.set_ylabel("Numero di Tweet")
        ax.set_title("Numero di Tweet per Settimana")

        ax.grid(True)
        st.pyplot(fig)

    elif analysis_option == "Per Mese":
        st.subheader("Numero di Tweet per Mese")
        st.write(df_month)

        # Grafico per mese
        fig, ax = plt.subplots()
        ax.plot(df_month['month'], df_month['count'], marker='o', color='r', label='Tweet per mese')
        ax.set_xlabel("Mese")
        ax.set_ylabel("Numero di Tweet")
        ax.set_title("Numero di Tweet per Mese")

        ax.grid(True)
        st.pyplot(fig)

