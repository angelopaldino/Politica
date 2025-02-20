import streamlit as st
import matplotlib.pyplot as plt
from Politica.backend.src.analisidelsentiment.sentiment import process_data2

# Lista dei candidati
candidates = [
    "Joe Biden", "Donald Trump", "Bernie Sanders", "Elizabeth Warren", "Kamala Harris",
    "Pete Buttigieg", "Cory Booker", "Andrew Yang", "Beto O’Rourke"
]

# Funzione per visualizzare il grafico a torta
def plot_pie_chart(sentiment_percentages):
    labels = sentiment_percentages.keys()
    sizes = sentiment_percentages.values()

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['green', 'red', 'gray'])
    ax.axis('equal')

    st.pyplot(fig)

def analisi2():

    st.title("Analisi del Sentiment sui Tweet")
    st.write("Scegli un candidato per eseguire l'analisi del sentiment sui tweet.")


    selected_candidate = st.selectbox("Seleziona un candidato", candidates)

    # Percorso del dataset
    input_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"

    if st.button("Esegui Analisi"):
        st.write(f"Analizzando i tweet per {selected_candidate}...")


        candidate_keywords = [selected_candidate.lower(), selected_candidate.split()[1].lower()]


        sentiment_percentages = process_data2(input_path, candidate_keywords)

        if sentiment_percentages:
            st.write(f"Percentuali di sentiment per {selected_candidate}:")
            st.write(sentiment_percentages)


            plot_pie_chart(sentiment_percentages)
        else:
            st.write("Nessun dato disponibile per l'analisi.")

