from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("klaxons").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.csv("hdfs://namenode:9000/data/lastfm/raw/ecoutes_historique.tsv", sep="\t", header=False, quote="")
(df.filter(F.lower(F.col("_c3")) == "klaxons")
   .groupBy("_c0").count()
   .orderBy(F.desc("count"))
   .show(5))