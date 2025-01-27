import streamlit as st

from Politica.frontend.src.Analisideicontenuti.CountWords import Tweets


def main():
    st.title("Benvenuto nell'app per le Elezioni Politiche in America!")
    Tweets()


if __name__ == "__main__":
    main()
