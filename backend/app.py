import streamlit as st
from pyspark.sql import SparkSession


def get_spark_session():
    if "spark" not in st.session_state:
        st.session_state.spark = SparkSession.builder \
            .appName("BigData Application") \
            .master("local[*]") \
            .getOrCreate()
            #.config("spark.driver.memory", "6g") \
            #.config("spark.executor.memory", "6g") \
    return st.session_state.spark


def stop_spark_session():
    if "spark" in st.session_state:
        st.session_state.spark.stop()
        del st.session_state.spark