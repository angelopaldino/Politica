
import streamlit as st

##DIRECTORY ANGELO
input_directory = "C:\\Users\\angel\\OneDrive\\Desktop\\Dataset parquet\\dataset\\dataset"
##
def app():
    st.write("Benvenuto")


    # Titolo dell'applicazione
    st.title("Visualizzazione Dati Twitter")


def display_home():
    st.title("Home Page")
    st.write("Benvenuto nella pagina iniziale del nostro progetto.")


def main():
    app()

if __name__ == "__main__":
    main()
