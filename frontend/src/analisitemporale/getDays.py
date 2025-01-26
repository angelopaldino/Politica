from datetime import datetime
import streamlit as st
from Politica.backend.src.Analisitemporale.get_data_for_date import get_data_for_day
import os


def getDays():
    st.title("Analisi dei Tweet per Giorno")

    # Campo di input per la data
    target_data = st.text_input("Inserisci una data (yyyy-mm-dd):")
    chunk_size = st.slider("Seleziona il numero di righe da visualizzare per volta", min_value=5, max_value=50, step=5, value=50)

    # Opzione per selezionare cosa visualizzare
    view_option = st.selectbox("Cosa vuoi visualizzare?", ["Tweet del giorno", "Hashtags", "Numero di retweet", "All"])

    # Percorsi dei dati
    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset"
    output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

    # Verifica che i percorsi siano validi
    if not os.path.exists(input_path):
        st.error(f"Il percorso di input non esiste: {input_path}")
        return
    if not os.path.exists(output_path):
        st.error(f"Il percorso di output non esiste: {output_path}")
        return

    # Bottone per avviare l'elaborazione
    if st.button("Analizza"):
        # Verifica che la data sia stata inserita correttamente
        if not target_data:
            st.error("Inserisci una data prima di procedere.")
            return

        try:
            # Convalida la data inserita
            target_data_obj = datetime.strptime(target_data, "%Y-%m-%d")
            st.write(f"Elaborando i dati per il giorno: {target_data_obj.strftime('%Y-%m-%d')}")
            target_data_str = target_data_obj.strftime("%Y-%m-%d")
        except ValueError:
            st.error("La data inserita non è nel formato corretto. Usa yyyy-mm-dd.")
            return

        # Chiamata alla funzione backend
        result_df = get_data_for_day(input_path, output_path, target_data_str)

        if result_df is None:
            st.error("Si è verificato un errore durante il recupero dei dati.")
        elif result_df.count() == 0:
            st.warning("Nessun dato trovato per questa data.")
        else:
            # Filtra i dati in base alla scelta dell'utente
            if view_option == "Tweet del giorno":
                result_df = result_df.select("created_at", "text")  # Colonne per i tweet
            elif view_option == "Hashtags":
                result_df = result_df.select("created_at", "hashtags")  # Colonne per gli hashtag
            elif view_option == "Numero di retweet":
                result_df = result_df.select("retweet_count")
            elif view_option == "All":
                result_df = result_df.select("created_at", "text", "hashtags", "retweet_count")  # Tutti i dati

            # Mostra i dati
            st.write(f"Visualizzazione dei dati per {target_data_str} ({view_option}):")
            total_rows = result_df.count()

            # Mostra il primo chunk
            result_df_pandas = result_df.limit(chunk_size).toPandas()
            st.dataframe(result_df_pandas)

            # Gestione del caricamento dei dati successivi
            if total_rows > chunk_size:
                st.write(f"Ci sono più dati da visualizzare (totale righe: {total_rows}).")
                if st.button("Carica più dati"):
                    start_index = chunk_size
                    remaining_df = result_df.limit(start_index, total_rows).toPandas()
                    st.dataframe(remaining_df)


