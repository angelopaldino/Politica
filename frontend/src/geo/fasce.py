import streamlit as st
import matplotlib.pyplot as plt

from Politica.backend.src.geo.fasce import fascia_linguistica, fascia_geografica


def fasce():
    st.title("Analisi Geografica")

    metodo = st.selectbox("Seleziona il metodo da eseguire:", [
        "Lingue più comuni", "Posizioni più comuni"
    ])

    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"


    if metodo == "Lingue più comuni":
        dataf = fascia_linguistica(input_path)
        df = dataf.limit(10).toPandas()
        st.subheader("Distribuzione delle lingue più comuni")

        fig, ax = plt.subplots(figsize=(10, 6))

        ax.bar(df['lang'], df['count'], color='skyblue', edgecolor='black')

        ax.set_xlabel('Lingua', fontsize=12)
        ax.set_ylabel('Conteggio', fontsize=12)
        ax.set_title('Top 10 lingue più comuni', fontsize=14)
        ax.tick_params(axis='x', rotation=45)

        st.pyplot(fig)


    elif metodo == "Posizioni più comuni":
        dataf = fascia_geografica(input_path)
        df = dataf.limit(10).toPandas()
        st.subheader("Distribuzione geografica dei posti")

        fig, ax = plt.subplots(figsize=(10, 6))

        ax.bar(df['place_name'], df['count'], color='skyblue', edgecolor='black')

        ax.set_xlabel('Lingua', fontsize=12)
        ax.set_ylabel('Conteggio', fontsize=12)
        ax.set_title('Top 10 lingue più comuni', fontsize=14)
        ax.tick_params(axis='x', rotation=45)

        st.pyplot(fig)


