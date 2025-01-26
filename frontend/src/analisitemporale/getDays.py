from datetime import datetime
import streamlit as st
from Politica.backend.src.Analisitemporale.get_data_for_date import get_data_for_day


def getDays():
    st.title("Analisi dei Tweet per Giorno")

    # Opzione 2: Data tramite input manuale (testo)
    target_data = st.text_input("Inserisci una data (yyyy-mm-dd):")

    # Verifica che la data non sia vuota prima di tentare la conversione
    if target_data:
        try:
            # Controlla se la data inserita è nel formato corretto
            target_data_obj = datetime.strptime(target_data, "%Y-%m-%d")
            st.write(f"La data inserita è: {target_data_obj.strftime('%Y-%m-%d')}")
        except ValueError:
            target_data_obj = None
            st.error("La data inserita non è nel formato corretto. Usa yyyy-mm-dd.")
    else:
        target_data_obj = None

    # Se la conversione è avvenuta correttamente, mostra la data selezionata
    if target_data_obj:
        st.write(f"La data selezionata è: {target_data_obj.strftime('%Y-%m-%d')}")
        chunk_size = st.slider("Seleziona il numero di righe da visualizzare per volta", min_value=5, max_value=50, step=5, value=50)

    # Percorsi dei dati
        input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset"
        output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

        if st.button("Analizza"):
            st.write(f"Elaborando i dati per il giorno: {target_data_obj.strftime('%Y-%m-%d')}")

            # Passa la data formattata come stringa alla funzione di backend
            target_data_str = target_data_obj.strftime("%Y-%m-%d")

            # Chiamata alla funzione backend
            result_df = get_data_for_day(input_path, output_path, target_data_str)

            if result_df is None:
                st.write("Si è verificato un errore durante il recupero dei dati.")
            if result_df.count() == 0:
                st.write("Nessun dato trovato per questa data.")
            else:
                st.write(f"Visualizzazione dei dati per {target_data_str}:")

                total_rows = result_df.count()

                # Mostra solo il primo chunk
                result_df_pandas = result_df.limit(chunk_size).toPandas()
                st.dataframe(result_df_pandas)

                # Gestione del caricamento dei dati successivi
                if total_rows > chunk_size:
                    # Se ci sono più righe, permette di caricare il chunk successivo
                    if st.button("Carica più dati"):
                        # Calcola l'indice di partenza per il prossimo batch
                        start_index = chunk_size
                        remaining_df = result_df.limit(start_index, total_rows).toPandas()
                        st.dataframe(remaining_df)
