import streamlit as st
from Politica.backend.src.Analisidegliutenti.frequenze import analyze_user_activity2


def user_activity2():

    st.title("Analisi Attività Utente")


    st.markdown("""
    Questo strumento permette di analizzare l'attività degli utenti nel dataset.
    Puoi visualizzare i tweet per utente, i tweet più retwittati e i tweet più favoriti.
    """)


    num_rows = st.number_input("Numero di righe da visualizzare", min_value=1, value=15, step=1)


    if st.button("Avvia Analisi Attività Utente"):

        with st.spinner("Sto eseguendo l'analisi..."):

            input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"
            output_path = "C:\\Users\\angel\\OneDrive\\Desktop\\TemporalAnalysis"


            user_activity_df = analyze_user_activity2(input_path, output_path)


            if user_activity_df is not None:
                st.success("Analisi completata con successo!")


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
        st.dataframe(user_activity_df.iloc[start_row:end_row])


        # Pulsante per caricare più righe
        if end_row < len(user_activity_df):
            if st.button('Carica altro'):
                st.session_state.start_row = end_row  # Aggiorna la riga di inizio
                st.experimental_rerun()  # Ricarica la pagina per mostrare i nuovi dati
