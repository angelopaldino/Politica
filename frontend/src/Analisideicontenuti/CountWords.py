import streamlit as st

from Politica.backend.src.Analisideicontenuti.CountsWords import count_words_for_candidate_in_chunks


def Tweets():
    st.markdown("<h1 style='text-align: center; color: #4CAF50;'>Analisi dei Tweet per Candidati</h1>", unsafe_allow_html=True)
    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"

    st.markdown("""
    Benvenuto nella pagina di analisi dei tweet! Qui puoi inserire il nome di un candidato per eseguire l'analisi delle parole più comuni.
    """)

    col1, col2 = st.columns(2)
    with col1:
        candidate = st.text_input("Inserisci il nome del candidato (es. 'Biden', 'Trump'):", "")
    with col2:
        st.text(f"Percorso dataset: {input_path}")

    st.markdown("---")

    if st.button("Esegui Analisi", key="start_analysis"):
        if candidate and input_path:
            st.markdown(f"### Esecuzione dell'analisi per: **{candidate}**")
            st.markdown(f"Analizzando il dataset in: **{input_path}**")

            with st.spinner("Elaborazione in corso..."):
                candidate_keywords = [candidate]
                results = count_words_for_candidate_in_chunks(input_path, candidate_keywords)

            if results is not None and not results.empty:
                st.success("Analisi completata con successo!")
                st.markdown("### Risultati dell'analisi:")
                st.dataframe(results)

                st.markdown("### Grafico delle parole più comuni:")
                st.bar_chart(data=results.set_index("words").head(10)["count"])
            else:
                st.error("Nessun risultato trovato per il candidato specificato.")
        else:
            st.error("Per favore, inserisci sia il nome del candidato che il percorso del dataset.")
