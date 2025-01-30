from datetime import datetime
import streamlit as st
from Politica.backend.src.Analisitemporale.days import get_data_for_day2

def getDays2():
    st.title("Analisi dei Tweet per Giorno")

    # Campo di input per la data
    ottobre_2020 = [f"2020-10-{str(i).zfill(2)}" for i in range(1, 32)]

    # Menu a tendina per la selezione della data
    target_data = st.selectbox("Seleziona una data:", ottobre_2020)

    # Opzione per selezionare cosa visualizzare
    view_option = st.selectbox("Cosa vuoi visualizzare?", ["Tweet del giorno", "Hashtags", "Numero di retweet", "All"])

    # Percorsi dei dati
    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"
    output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

    # Variabili di sessione per risultati e indice
    if "result_df" not in st.session_state:
        st.session_state.result_df = None
    if "current_index" not in st.session_state:
        st.session_state.current_index = 0

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
        result_df = get_data_for_day2(input_path, output_path, target_data_str)

        if result_df is None:
            st.error("Si è verificato un errore durante il recupero dei dati.")
        elif result_df.count() == 0:
            st.warning("Nessun dato trovato per questa data.")
        else:
            # Salva il risultato nella sessione
            st.session_state.result_df = result_df
            st.session_state.current_index = 0  # Reset dell'indice

    # Mostra i dati salvati
    if st.session_state.result_df is not None:
        result_df = st.session_state.result_df

        # Filtra i dati in base alla scelta dell'utente
        if view_option == "Tweet del giorno":
            result_df = result_df.select("created_at", "text")
        elif view_option == "Hashtags":
            result_df = result_df.select("created_at", "hashtags")
        elif view_option == "Numero di retweet":
            result_df = result_df.select("retweet_count")
        elif view_option == "All":
            result_df = result_df.select("created_at", "text", "hashtags", "retweet_count")

        # Calcola il totale e l'indice corrente
        total_rows = result_df.count()
        start_index = st.session_state.current_index
        chunk_size = 20  # Numero di righe da visualizzare alla volta
        end_index = min(start_index + chunk_size, total_rows)

        # Carica il chunk corrente e rimuove valori NA
        result_df_pandas = result_df.limit(end_index).toPandas()
        result_df_pandas = result_df_pandas.iloc[start_index:end_index]
        result_df_pandas = result_df_pandas.dropna(how="any")

        # Mostra i dati in una tabella
        st.dataframe(result_df_pandas, use_container_width=True)

        # Aggiorna l'indice corrente
        if end_index < total_rows:
            if st.button("Carica più dati"):
                st.session_state.current_index = end_index
