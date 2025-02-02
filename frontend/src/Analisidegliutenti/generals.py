
from pyspark.sql.functions import col

import streamlit as st
import matplotlib.pyplot as plt

from Politica.backend.src.Analisidegliutenti.generals import utenti_piu_taggati,post_da_utente, fascia_utente


def generals():
    st.title("Analisi generali")
    st.markdown("Questo strumento permette eseguire le seguenti funzionalità: vedere gli utenti per fascia oraria"
                ", utenti più taggati  e i post di un utente")

    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"

    analisi_utente = st.selectbox("Seleziona il tipo di analisi:", [
        "Utenti taggati", "Pubblicazioni", "Post da utente"
    ])

    if analisi_utente == "Utenti taggati":
        dataf = utenti_piu_taggati(input_path)
        df = dataf.limit(10).toPandas()
        plt.figure(figsize=(10, 6))
        plt.bar(df['in_reply_to_screen_name'][:10], df['count'][:10], color='skyblue', edgecolor='black')

        plt.title("Utenti più taggati (Top 10)")
        plt.xlabel("Utente")
        plt.ylabel("Tag")

        plt.xticks(rotation=45, ha="right")

        st.pyplot(plt)


    elif analisi_utente == "Pubblicazioni":
        dataf = fascia_utente(input_path)
        df = dataf.limit(10).toPandas()
        plt.figure(figsize=(10, 6))
        plt.bar(df['screen_name'][:10], df['count'][:10], color='skyblue', edgecolor='black')

        plt.title("Pubblicazioni per utente (Top 10)")
        plt.xlabel("Utente")
        plt.ylabel("Frequenza")

        plt.xticks(rotation=45, ha="right")

        st.pyplot(plt)


    elif analisi_utente == "Post da utente":
        # Recupera lo username inserito dall'utente
        keyword_input = st.text_input("Inserisci lo username da cercare", "", key="u1")

        if keyword_input:
            keyword = keyword_input.strip().lower()  # Normalizza l'input dell'utente

            # Debug: Mostra la keyword dopo la normalizzazione
            st.write(f"Keyword cercata: '{keyword}'")

            # Inizializza o aggiorna il limite e la keyword precedenti
            if keyword != st.session_state.prev_keyword:
                st.session_state.limit_user = 100
                st.session_state.prev_keyword = keyword

            # Chiamata al backend con il parametro username
            dataf = post_da_utente(input_path, keyword)

            # Limita i risultati a 10 righe per la visualizzazione
            df = dataf.limit(st.session_state.limit_user).toPandas()

            # Debug: Mostra quanti risultati sono stati trovati
            #st.write(f"Numero di risultati trovati: {df.shape[0]}")

            # Debug: Stampa i primi 10 risultati trovati
            #st.write("Primi 10 risultati trovati:")
            #st.write(df.head(10))

            # Mostra i risultati filtrati
            if df.shape[0] > 0:
                st.subheader("Post dell'Utente Filtrati")
                st.dataframe(df)
            else:
                st.write(f"Nessun post trovato per lo username '{keyword_input}'.")

            # Estendi i risultati
            if st.button("Estendi risultato", key="bu1"):
                st.session_state.limit_user += 100
                st.experimental_rerun()

