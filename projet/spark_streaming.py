from pymongo import MongoClient, UpdateOne, ASCENDING
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

BROKER = "kafka:9092"
TOPIC = "ecoutes"
MONGO_URI = "mongodb://mongo:27017"
BASE = "streaming_musique"
COLLECTION = "agregats_temps_reel"
CHECKPOINT = "/home/jovyan/checkpoints/agregats_temps_reel"
TAILLE_FENETRE = "5 minutes"
PAS_FENETRE = "1 minute"
RETARD_MAX = "1 minute"
FORMAT_ISO = "yyyy-MM-dd'T'HH:mm:ss'Z'"

spark = (SparkSession.builder
         .appName("brique-spark-streaming")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

collection = MongoClient(MONGO_URI)[BASE][COLLECTION]
collection.create_index([("fenetre_debut", ASCENDING), ("cle_musique", ASCENDING)], unique=True)

schema_message = StructType([StructField(c, StringType(), True) for c in
                             ["user_id", "horodatage_origine", "horodatage_envoi",
                              "nom_artiste", "nom_musique", "cle_musique"]])

brut = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load())

ecoutes = (brut
           .select(F.from_json(F.col("value").cast("string"), schema_message).alias("m"))
           .select("m.*")
           .withColumn("temps_envoi", F.to_timestamp("horodatage_envoi", FORMAT_ISO)))

agregats = (ecoutes
            .withWatermark("temps_envoi", RETARD_MAX)
            .groupBy(F.window("temps_envoi", TAILLE_FENETRE, PAS_FENETRE), "cle_musique")
            .agg(F.count("*").alias("nb_ecoutes"),
                 F.first("nom_artiste").alias("nom_artiste"),
                 F.first("nom_musique").alias("nom_musique"))
            .select(F.date_format("window.start", FORMAT_ISO).alias("fenetre_debut"),
                    F.date_format("window.end", FORMAT_ISO).alias("fenetre_fin"),
                    "cle_musique", "nom_artiste", "nom_musique", "nb_ecoutes"))


def ecrire_mongo(batch_df, batch_id):
    lignes = [r.asDict() for r in batch_df.collect()]
    if not lignes:
        return

    operations = [UpdateOne({"fenetre_debut": l["fenetre_debut"], "cle_musique": l["cle_musique"]},
                            {"$set": l}, upsert=True)
                  for l in lignes]
    collection.bulk_write(operations, ordered=False)

    top = sorted(lignes, key=lambda l: l["nb_ecoutes"], reverse=True)[:3]
    print(f"Batch {batch_id} : {len(lignes)} documents écrits dans {COLLECTION}")
    for l in top:
        print(f"   [{l['fenetre_debut'][11:16]} -> {l['fenetre_fin'][11:16]}] "
              f"{l['nom_artiste']} - {l['nom_musique']} : {l['nb_ecoutes']} écoute(s)")


requete = (agregats.writeStream
           .outputMode("update")
           .foreachBatch(ecrire_mongo)
           .option("checkpointLocation", CHECKPOINT)
           .trigger(processingTime="10 seconds")
           .start())

print("Streaming démarré, en attente des messages du topic", TOPIC, "(Ctrl+C pour arrêter)")
requete.awaitTermination()
