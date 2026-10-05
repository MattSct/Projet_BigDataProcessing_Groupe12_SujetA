from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

HDFS_PATH = "hdfs://namenode:9000/data/lastfm/raw/ecoutes_historique.tsv"
MONGO_URI = "mongodb://mongo:27017"
BASE = "streaming_musique"
COLLECTION = "recommandations"
MIN_AUDITEURS = 2
NB_RECOS = 10

spark = (SparkSession.builder
         .appName("brique-als")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

colonnes = ["user_id", "horodatage", "id_artiste", "nom_artiste",
            "id_musique", "nom_musique", "cle_musique"]
schema = StructType([StructField(c, StringType(), True) for c in colonnes])
ecoutes = spark.read.csv(HDFS_PATH, schema=schema, sep="\t", header=False, quote="")

notes = (ecoutes
         .groupBy("user_id", "cle_musique")
         .agg(F.count("*").alias("nb_ecoutes")))

musiques_partagees = (notes
                      .groupBy("cle_musique")
                      .agg(F.countDistinct("user_id").alias("nb_auditeurs"))
                      .filter(F.col("nb_auditeurs") >= MIN_AUDITEURS)
                      .select("cle_musique"))

notes = (notes
         .join(musiques_partagees, "cle_musique")
         .withColumn("note", F.log1p("nb_ecoutes")))
notes.cache()
print("Paires (utilisateur, musique) gardées :", notes.count())
print("Utilisateurs :", notes.select("user_id").distinct().count(),
      "| Musiques :", notes.select("cle_musique").distinct().count())

indexeur_user = StringIndexer(inputCol="user_id", outputCol="user_idx").fit(notes)
indexeur_musique = StringIndexer(inputCol="cle_musique", outputCol="musique_idx").fit(notes)
donnees = (indexeur_musique.transform(indexeur_user.transform(notes))
           .withColumn("user_idx", F.col("user_idx").cast("int"))
           .withColumn("musique_idx", F.col("musique_idx").cast("int")))
donnees.cache()

als = ALS(userCol="user_idx", itemCol="musique_idx", ratingCol="note",
          implicitPrefs=True, rank=10, maxIter=10, regParam=0.1,
          coldStartStrategy="drop", seed=42)
modele = als.fit(donnees)
print("Modèle ALS entraîné")

recos = (modele.recommendForAllUsers(NB_RECOS + 1000)
         .select("user_idx", F.explode("recommendations").alias("r"))
         .select("user_idx",
                 F.col("r.musique_idx").alias("musique_idx"),
                 F.col("r.rating").alias("score")))

deja_ecoutees = donnees.select("user_idx", "musique_idx")

infos_musiques = (ecoutes
                  .groupBy("cle_musique")
                  .agg(F.first("nom_artiste").alias("nom_artiste"),
                       F.first("nom_musique").alias("nom_musique")))
table_users = donnees.select("user_idx", "user_id").distinct()
table_musiques = (donnees.select("musique_idx", "cle_musique").distinct()
                  .join(infos_musiques, "cle_musique"))

rang = Window.partitionBy("user_idx").orderBy(F.desc("score"))
top = (recos
       .join(deja_ecoutees, ["user_idx", "musique_idx"], "left_anti")
       .withColumn("rang", F.row_number().over(rang))
       .filter(F.col("rang") <= NB_RECOS)
       .join(table_users, "user_idx")
       .join(table_musiques, "musique_idx")
       .select("user_id", "rang", "cle_musique", "nom_artiste", "nom_musique", "score"))

par_user = {}
for l in top.collect():
    par_user.setdefault(l["user_id"], []).append(l)

date_calcul = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
documents = []
for user_id, lignes in sorted(par_user.items()):
    lignes.sort(key=lambda l: l["rang"])
    documents.append({
        "_id": user_id,
        "recommandations": [{"cle_musique": l["cle_musique"],
                             "nom_artiste": l["nom_artiste"],
                             "nom_musique": l["nom_musique"],
                             "score": round(float(l["score"]), 4)}
                            for l in lignes],
        "date_calcul": date_calcul,
    })

collection = MongoClient(MONGO_URI)[BASE][COLLECTION]
collection.delete_many({})
collection.insert_many(documents)
print(f"{len(documents)} utilisateurs ont reçu {NB_RECOS} recommandations dans {BASE}.{COLLECTION}")

exemple = documents[0]["_id"]
print(f"\nCe que {exemple} écoute le plus :")
for l in (notes.filter(F.col("user_id") == exemple)
          .orderBy(F.desc("nb_ecoutes")).limit(5).collect()):
    print(f"   {l['cle_musique']} ({l['nb_ecoutes']} écoutes)")
print(f"Ce qu'on lui recommande :")
for r in documents[0]["recommandations"][:5]:
    print(f"   {r['nom_artiste']} - {r['nom_musique']} (score {r['score']})")

spark.stop()
