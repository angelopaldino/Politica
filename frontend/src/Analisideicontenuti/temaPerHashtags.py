from collections import defaultdict
import matplotlib.pyplot as plt
# Dizionario di parole chiave per i temi
themes_keywords = {
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
    theme_counts = defaultdict(int)  # Conta gli hashtag per tema

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            hashtag, count = line.strip().split(',')  # Supponiamo che ogni riga sia "hashtag,count"
            theme = classify_hashtag(hashtag, themes_keywords)

            if theme:
                theme_counts[theme] += int(count)  # Incrementa il conteggio per il tema

    return theme_counts

# Funzione per visualizzare i risultati in un grafico a torta migliorato


def plot_pie_chart(theme_counts):
    # Preparazione dei dati
    labels = list(theme_counts.keys())
    sizes = list(theme_counts.values())


    colors = ['red','blue','green','black']


    plt.figure(figsize=(8, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors, wedgeprops={'edgecolor': 'black'})


    plt.title('Distribuzione Temi degli Hashtags')


    plt.axis('equal')



# Percorso del file txt contenente gli hashtag e i loro conteggi
file_path = "C:\\Users\\angel\\OneDrive\\Desktop\\Big Data\\output\\hashtags_output.txt"


theme_counts = analyze_hashtags(file_path, themes_keywords)

# Visualizza il grafico a torta
plot_pie_chart(theme_counts)

