from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.csv("hdfs://namenode:9000/data/lastfm/raw/ecoutes_historique.tsv", sep="\t", header=False)
print(df.count())
df.show(3, truncate=False)