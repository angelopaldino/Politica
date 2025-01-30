import streamlit as st
from Politica.backend.src.Analisidegliutenti.frequenze import analyze_user_activity2


def user_activity2():
    # Titolo della pagina
    st.title("Analisi Attività Utente")

    # Descrizione dell'analisi
    st.markdown("""
    Questo strumento permette di analizzare l'attività degli utenti nel dataset.
    Puoi visualizzare i tweet per utente, i tweet più retwittati e i tweet più favoriti.
    """)

    # Imposta il numero di righe da visualizzare
    num_rows = st.number_input("Numero di righe da visualizzare", min_value=1, value=15, step=1)

    # Aggiungi un pulsante per avviare l'analisi
    if st.button("Avvia Analisi Attività Utente"):
        # Mostra il messaggio di stato durante l'analisi
        with st.spinner("Sto eseguendo l'analisi..."):
            # Carica i dati di attività utente
            input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"
            output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"

            # Chiamata al backend per analizzare l'attività utente
            user_activity_df = analyze_user_activity2(input_path, output_path)

            # Verifica se l'analisi è riuscita
            if user_activity_df is not None:
                st.success("Analisi completata con successo!")

                # Salva il dataframe completo in sessione
                st.session_state.user_activity_df = user_activity_df
                st.session_state.start_row = 0  # Iniziamo dalla prima riga
                st.write("Dati dell'attività utente:")
                st.dataframe(user_activity_df.head(num_rows))  # Mostra le prime righe

    # Gestione della paginazione
    if 'user_activity_df' in st.session_state:
        user_activity_df = st.session_state.user_activity_df
        start_row = st.session_state.start_row
        end_row = start_row + num_rows

        # Mostra il dataframe paginato
        st.write(user_activity_df.iloc[start_row:end_row])

        # Pulsante per caricare più righe
        if end_row < len(user_activity_df):
            if st.button('Carica altro'):
                st.session_state.start_row = end_row  # Aggiorna la riga di inizio
                st.experimental_rerun()  # Ricarica la pagina per mostrare i nuovi dati
