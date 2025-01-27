from pyspark import SparkContext
sc = SparkContext.getOrCreate()
sc.addPyFile("path_to_vaderSentiment.zip")

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()
