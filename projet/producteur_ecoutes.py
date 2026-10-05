import argparse
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "ecoutes"
BROKER = "kafka:9092"
FICHIER = "/home/jovyan/work/ecoutes_flux.tsv"


def maintenant_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--debit", type=float, default=20)
    ap.add_argument("--limite", type=int, default=0)
    args = ap.parse_args()

    producteur = KafkaProducer(bootstrap_servers=BROKER)

    pause = 1 / args.debit
    nb_envoyes = 0
    debut = time.time()
    print(f"Envoi vers le topic '{TOPIC}' ({BROKER}) à {args.debit:g} messages/s, Ctrl+C pour arrêter")

    try:
        with open(FICHIER, encoding="utf-8") as f:
            for ligne in f:
                champs = ligne.rstrip("\n").split("\t")
                if len(champs) != 7:
                    continue

                message = {
                    "user_id": champs[0],
                    "horodatage_origine": champs[1],
                    "horodatage_envoi": maintenant_iso(),
                    "nom_artiste": champs[3],
                    "nom_musique": champs[5],
                    "cle_musique": champs[6],
                }
                producteur.send(TOPIC,
                                key=champs[0].encode("utf-8"),
                                value=json.dumps(message, ensure_ascii=False).encode("utf-8"))
                nb_envoyes += 1

                if nb_envoyes % 200 == 0:
                    print(f"  {nb_envoyes} messages envoyés")
                if args.limite and nb_envoyes >= args.limite:
                    break

                attente = debut + nb_envoyes * pause - time.time()
                if attente > 0:
                    time.sleep(attente)
    except KeyboardInterrupt:
        print("\nArrêt demandé")
    finally:
        producteur.flush()
        producteur.close()
        print(f"Terminé : {nb_envoyes} messages envoyés en {time.time() - debut:.0f} s")


if __name__ == "__main__":
    main()
