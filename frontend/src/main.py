import streamlit as st

from Politica.frontend.src.Analisideicontenuti.CountWords import Tweets
from Politica.frontend.src.Analisidelsentiment.sentimentAnalysis import analisi


def main():
    st.title("Benvenuto nell'app per le Elezioni Politiche in America!")
    #Tweets()
    analisi()




if __name__ == "__main__":
    main()
