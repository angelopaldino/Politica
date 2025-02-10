import streamlit as st
import pydeck as pdk
import os
import pandas as pd
from Politica.backend.src.geo.geolocalizzazione import geo


def show_map():
    st.title("Mappa dei Tweet Geolocalizzati")
    st.write("Questa sezione ti permette di visualizzare i tweet in base alla loro geolocalizzazione.")

    data_path = "C:\\Users\\angel\\OneDrive\\Desktop\\subsetdataset\\dataset_sottoinsieme\\dataset_sottoinsieme"


    df_geo = geo(data_path)

    # Verifica se ci sono dati disponibili
    if df_geo is None or df_geo.empty:
        st.error("Nessun dato disponibile per la mappa.")
        return

    # Converte le colonne di latitudine e longitudine in formato numerico
    df_geo["place_lat"] = pd.to_numeric(df_geo["place_lat"], errors='coerce')
    df_geo["place_lon"] = pd.to_numeric(df_geo["place_lon"], errors='coerce')

    # Rimuove righe con valori NaN nelle coordinate
    df_geo = df_geo.dropna(subset=["place_lat", "place_lon"])

    # Verifica che i dati siano validi
    if df_geo.empty:
        st.error("Dati geografici non validi o incompleti.")
        return


    st.write(
        "Questa mappa mostra la geolocalizzazione dei tweet analizzati, "
        "visualizzando la distribuzione dei tweet in base alle coordinate geografiche."
    )

    # Crea la mappa con Pydeck
    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=pdk.ViewState(
            latitude=df_geo["place_lat"].mean(),
            longitude=df_geo["place_lon"].mean(),
            zoom=3,
            pitch=50,
        ),
        layers=[
            pdk.Layer(
                "HexagonLayer",
                data=df_geo,
                get_position=["place_lon", "place_lat"],
                radius=10000,  # Dimensione degli esagoni
                elevation_scale=50,
                elevation_range=[0, 3000],
                pickable=True,
                extruded=True,
            ),
            pdk.Layer(
                "ScatterplotLayer",
                data=df_geo,
                get_position=["place_lon", "place_lat"],
                get_color="[200, 30, 0, 160]",
                get_radius=5000,
            ),
        ],
    ))

    st.write("Mappa generata con i dati raccolti.")