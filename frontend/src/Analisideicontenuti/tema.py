import streamlit as st
import matplotlib.pyplot as plt
from collections import defaultdict


# Dizionario di parole chiave per i temi
themes_keywords2 = {
    "politica": ["vote", "election", "president", "senate", "democracy", "campaign", "politics"],
    "tech": ["tech", "AI", "machinelearning", "robotics", "iot", "cloud", "5G", "blockchain"],
    "covid": ["covid", "pandemic", "coronavirus", "lockdown", "quarantine", "vaccination"],
    "health": ["health", "fitness", "medicine", "wellness", "mentalhealth", "nutrition", "exercise"],
}

# Funzione per determinare il tema di un hashtag
def classify_hashtag(hashtag, themes_keywords):
    hashtag = hashtag.lower().strip()
    for theme, keywords in themes_keywords.items():
        if any(keyword in hashtag for keyword in keywords):
            return theme
    return None  # Se non corrisponde a nessun tema

# Funzione per analizzare il file txt e classificare gli hashtag
def analyze_hashtags(file_path, themes_keywords):
    theme_counts2 = defaultdict(int)  # Conta gli hashtag per tema

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            hashtag, count = line.strip().split(',')  # Supponiamo che ogni riga sia "hashtag,count"
            theme = classify_hashtag(hashtag, themes_keywords)

            if theme:
                theme_counts2[theme] += int(count)  # Incrementa il conteggio per il tema

    return theme_counts2


def plot_pie_chart(theme_counts):
    # Preparazione dei dati
    labels = list(theme_counts.keys())
    sizes = list(theme_counts.values())


    colormap = plt.cm.get_cmap('Set3', len(labels))
    colors = [colormap(i) for i in range(len(labels))]  # Assegna un colore unico per ogni tema

    # Crea il grafico a torta
    plt.figure(figsize=(10, 7))  # Aggiungi figsize per dimensioni personalizzate
    wedges, _, autotexts = plt.pie(sizes, autopct='%1.1f%%', startangle=140, colors=colors, wedgeprops={'edgecolor': 'black', 'linewidth': 1.5}, explode=(0.1, 0, 0, 0))  # Effetto "esplosione"


    for autotext in autotexts:
        autotext.set_fontsize(14)  # Aumenta la dimensione del font per le percentuali
        autotext.set_weight('bold')  # Rendi il testo in grassetto per maggiore visibilità
        autotext.set_color('white')  # Colore bianco per il testo delle percentuali per un buon contrasto




    plt.title('Distribuzione Temi degli Hashtags', fontsize=16, fontweight='bold')


    plt.legend(wedges, labels, title="Temi", loc="upper right", fontsize=10)


    st.pyplot(plt)

