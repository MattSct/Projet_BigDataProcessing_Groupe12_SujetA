from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

#PARAM
HDFS_PATH = "hdfs://namenode:9000/data/lastfm/raw/ecoutes_historique.tsv"
MONGO_URI = "mongodb://mongo:27017"
BASE = "streaming_musique"
COLLECTION = "stats_historiques"
TOP_N = 10

spark = (SparkSession.builder
         .appName("brique-spark-batch")
         .config("spark.mongodb.write.connection.uri", MONGO_URI)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

#Lecture depuis HDFS en respectant le contrat  
colonnes = ["user_id", "horodatage", "id_artiste", "nom_artiste",
            "id_musique", "nom_musique", "cle_musique"]
schema = StructType([StructField(c, StringType(), True) for c in colonnes])

ecoutes = spark.read.csv(HDFS_PATH, schema=schema, sep="\t",
                         header=False, quote="")
ecoutes.cache()  # le DataFrame est réutilisé 3 fois : on le garde en mémoire
print("Nombre d'écoutes lues :", ecoutes.count())

#Top artistes
top_artistes = (ecoutes
    .withColumn("nom_artiste", F.lower(F.trim("nom_artiste")))
    .groupBy("nom_artiste")
    .agg(F.count("*").alias("nb_ecoutes"))
    .withColumn("rang", F.row_number().over(
        Window.orderBy(F.desc("nb_ecoutes"), "nom_artiste")))
    .filter(F.col("rang") <= TOP_N)
    .withColumn("type", F.lit("top_artistes"))
    .select("type", "rang", "nom_artiste", "nb_ecoutes"))

#Top musiques 
top_musiques = (ecoutes
    .groupBy("cle_musique")
    .agg(F.count("*").alias("nb_ecoutes"),
         F.first("nom_artiste").alias("nom_artiste"),
         F.first("nom_musique").alias("nom_musique"))
    .withColumn("rang", F.row_number().over(
        Window.orderBy(F.desc("nb_ecoutes"), "cle_musique")))
    .filter(F.col("rang") <= TOP_N)
    .withColumn("type", F.lit("top_musiques"))
    .select("type", "rang", "cle_musique", "nom_artiste",
            "nom_musique", "nb_ecoutes"))

#Écoutes par heure 
ecoutes_par_heure = (ecoutes
    .withColumn("heure", F.substring("horodatage", 12, 2).cast("int"))
    .groupBy("heure")
    .agg(F.count("*").alias("nb_ecoutes"))
    .withColumn("type", F.lit("ecoutes_par_heure"))
    .select("type", "heure", "nb_ecoutes")
    .orderBy("heure"))

top_artistes.show(truncate=False)
top_musiques.show(truncate=False)
ecoutes_par_heure.show(24, truncate=False)

#  5. Écriture dans MongoDB 
def ecrire(df, mode):
    (df.write.format("mongodb")
       .mode(mode)
       .option("database", BASE)
       .option("collection", COLLECTION)
       .save())

ecrire(top_artistes, "overwrite")   #relancable sans doublon
ecrire(top_musiques, "append")
ecrire(ecoutes_par_heure, "append")
print("Écriture terminée dans", BASE + "." + COLLECTION)

spark.stop()