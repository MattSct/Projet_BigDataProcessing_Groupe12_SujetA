import argparse
import json
import os
import sys
from pathlib import Path

DB_NAME = "streaming_musique"
COLLECTION = "catalogue"


def lire(chemin, catalogue):
    """Parcourt un fichier d'écoutes et ajoute chaque musique distincte au catalogue."""
    nb_lignes = nb_ignorees = 0
    with open(chemin, encoding="utf-8") as f:
        for ligne in f:
            champs = ligne.rstrip("\n").split("\t")
            nb_lignes += 1
            if len(champs) != 7:
                nb_ignorees += 1
                continue
            nom_artiste, nom_musique, cle = champs[3], champs[5], champs[6]
            if not nom_artiste.strip() or not nom_musique.strip() or not cle.strip():
                nb_ignorees += 1
                continue
            if cle not in catalogue:   # on garde la 1re graphie rencontrée
                catalogue[cle] = {"_id": cle, "nom_artiste": nom_artiste, "nom_musique": nom_musique}
    print(f"  {chemin} : {nb_lignes} lignes lues, {nb_ignorees} ignorées (mal formées ou champs vides)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--historique", default=str(Path("data_local") / "ecoutes_historique.tsv"),
                    help="copie locale récupérée depuis HDFS (brique 1)")
    ap.add_argument("--flux", default="ecoutes_flux.tsv", help="écoutes rejouées dans Kafka")
    ap.add_argument("--sortie", default="catalogue.json")
    ap.add_argument("--import", dest="importer", action="store_true",
                    help="importer aussi dans MongoDB avec pymongo")
    args = ap.parse_args()

    catalogue = {}
    print("Lecture des fichiers :")
    if not Path(args.historique).exists():
        sys.exit(f"Fichier introuvable : {args.historique}")
    lire(args.historique, catalogue)
    nb_hist = len(catalogue)
    if Path(args.flux).exists():
        lire(args.flux, catalogue)
    else:
        print(f"  (flux absent : {args.flux} -> catalogue construit sur l'historique seul)")

    docs = sorted(catalogue.values(), key=lambda d: d["_id"])
    print(f"Musiques distinctes dans l'historique : {nb_hist}")
    print(f"Musiques au catalogue (historique + flux) : {len(docs)}")

    with open(args.sortie, "w", encoding="utf-8", newline="\n") as out:
        out.write("[\n")
        for i, d in enumerate(docs):
            out.write(("," if i else "") + json.dumps(d, ensure_ascii=False) + "\n")
        out.write("]\n")
    print(f"{args.sortie} ecrit : {Path(args.sortie).stat().st_size / 1e6:.1f} Mo")

    if args.importer:
        uri = os.environ.get("MONGO_URI")
        if not uri:
            sys.exit("Variable MONGO_URI absente. Sous cmd :  set \"MONGO_URI=mongodb+srv://...\"")
        from pymongo import MongoClient
        col = MongoClient(uri, serverSelectionTimeoutMS=8000)[DB_NAME][COLLECTION]
        col.drop()
        for i in range(0, len(docs), 5000):
            col.insert_many(docs[i:i + 5000], ordered=False)
        print(f"Import termine : {col.count_documents({})} documents dans {DB_NAME}.{COLLECTION}")


if __name__ == "__main__":
    main()
