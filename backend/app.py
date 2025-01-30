import streamlit as st
from pyspark.sql import SparkSession

# Funzione per inizializzare SparkSession
def get_spark_session():
    if "spark" not in st.session_state:
        st.session_state.spark = SparkSession.builder \
            .appName("BigData Application") \
            .master("local[4]") \
            .config("spark.sql.shuffle.partitions", "100") \
            .config("spark.sql.files.maxPartitionBytes", "128MB") \
            .getOrCreate()
    return st.session_state.spark

# Funzione per chiudere SparkSession
def stop_spark_session():
    if "spark" in st.session_state:
        st.session_state.spark.stop()
        del st.session_state.spark