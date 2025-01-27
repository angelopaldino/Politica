import streamlit as st
import pandas as pd
from Politica.backend.src.Analisidegliutenti.frequenzatweetutente import analyze_user_activity

def user_activity():
    # Titolo della pagina
    st.title("Analisi Attività Utente")

    # Descrizione dell'analisi
    st.markdown("""
    Questo strumento permette di analizzare l'attività degli utenti nel dataset.
    Puoi visualizzare i tweet per utente, i tweet più retwittati e i tweet più favoriti.
    """)

    # Aggiungi un pulsante per avviare l'analisi
    if st.button("Avvia Analisi Attività Utente"):
        # Mostra il messaggio di stato durante l'analisi
        with st.spinner("Sto eseguendo l'analisi..."):
            # Carica i dati di attività utente
            input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"
            output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

            # Chiamata al backend per analizzare l'attività utente
            user_activity_df = analyze_user_activity(input_path, output_path)

            # Verifica se l'analisi è riuscita
            if user_activity_df is not None:
                st.success("Analisi completata con successo!")
                st.write("Dati dell'attività utente:")
                st.dataframe(user_activity_df)  # Mostra i dati in una tabella

                # Grafico dei tweet per utente
                st.subheader("Numero di tweet per utente")
                tweet_count_chart = user_activity_df[['user_id_str', 'tweet_count']].sort_values(by='tweet_count', ascending=False)
                st.bar_chart(tweet_count_chart.set_index('user_id_str'))

                # Grafico dei tweet più retwittati
                st.subheader("Tweet più retwittati per utente")
                most_retweeted_chart = user_activity_df[['user_id_str', 'max_retweet_count']].sort_values(by='max_retweet_count', ascending=False)
                st.bar_chart(most_retweeted_chart.set_index('user_id_str'))

                # Grafico dei tweet più favoriti
                st.subheader("Tweet più favoriti per utente")
                most_favorited_chart = user_activity_df[['user_id_str', 'max_favorite_count']].sort_values(by='max_favorite_count', ascending=False)
                st.bar_chart(most_favorited_chart.set_index('user_id_str'))

            else:
                st.error("Si è verificato un errore durante l'analisi. Riprova.")



