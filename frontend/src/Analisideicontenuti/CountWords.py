import streamlit as st

from Politica.backend.src.Analisideicontenuti.CountsWords import count_words_for_candidate_in_chunks


def Tweets():
    # Personalizzazione del titolo con stile
    st.markdown("<h1 style='text-align: center; color: #4CAF50;'>Analisi dei Tweet per Candidati</h1>", unsafe_allow_html=True)
    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Datasetparquet\\dataset\\dataset"
    # Aggiungere un'immagine di benvenuto
    #st.image("https://upload.wikimedia.org/wikipedia/commons/4/47/Trump_vs_Biden_2020.svg", use_column_width=True)

    # Descrizione del progetto
    st.markdown("""
    Benvenuto nella pagina di analisi dei tweet! Qui puoi inserire il nome di un candidato  per eseguire l'analisi delle parole più comuni. """)

    # Layout con colonne per una disposizione più ordinata
    col1, col2 = st.columns(2)

    # Input per il nome del candidato nella colonna di sinistra
    with col1:
        candidate = st.text_input("Inserisci il nome del candidato (es. 'Biden', 'Trump'):", "")

    # Input per il percorso del dataset nella colonna di destra
    with col2:
        input_path

    # Aggiungere un separatore per migliorare la leggibilità
    st.markdown("---")

    # Bottone per avviare l'analisi
    if st.button("Esegui Analisi", key="start_analysis"):
        if candidate and input_path:
            # Messaggio di conferma dell'analisi
            st.markdown(f"### Esecuzione dell'analisi per: **{candidate}**")
            st.markdown(f"Analizzando il dataset in: **{input_path}**")

            # Aggiungi una barra di progresso per l'elaborazione
            with st.spinner("Elaborazione in corso..."):
                candidate_keywords = [candidate]  # Lista con il nome del candidato
                count_words_for_candidate_in_chunks(input_path, candidate_keywords)

            st.success("Analisi completata con successo!")
        else:
            st.error("Per favore, inserisci sia il nome del candidato che il percorso del dataset.")


