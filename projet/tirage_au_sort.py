import random

# CONSTANTES
FICHIER_SOURCE="userid-timestamp-artid-artname-traid-traname.tsv"
FICHIER_HDFS="ecoutes_historique.tsv" #écoutes avant la coupure -> HDFS
FICHIER_KAFKA="ecoutes_flux.tsv" #écoutes après la coupure -> Kafka
SEUIL_ECOUTES = 1000 #nombre minimal d'écoutes pour être éligible
NB_UTILISATEURS = 100 #nombre d'utilisateurs tirés au hasard
GRAINE = 42 #graine fixe=>meme tirage
DATE_COUPURE = "2009-05-01"


def normaliser(texte):
    """Minuscules + suppression des espaces en trop (début, fin, doubles)."""
    return " ".join(texte.lower().split())


#compter les écoutes de chaque utilisateur

nb_ecoutes={} #dictionnaire {user_id:nombre d'écoutes }

with open(FICHIER_SOURCE, encoding="utf-8") as f:
    for ligne in f:
        user_id = ligne.split("\t", 1)[0] #on ne découpe que la 1re colonne
        nb_ecoutes[user_id] = nb_ecoutes.get(user_id, 0) + 1

print(f"Utilisateurs dans le fichier : {len(nb_ecoutes)}")

#garder les utilisateurs actifs et tirage au sort

eligibles = sorted(u for u, n in nb_ecoutes.items() if n >= SEUIL_ECOUTES)
print(f"Utilisateurs avec au moins {SEUIL_ECOUTES} écoutes : {len(eligibles)}")

random.seed(GRAINE)
choisis = set(random.sample(eligibles, NB_UTILISATEURS))
print(f"Utilisateurs tirés au hasard : {len(choisis)}")


#relire le fichier, arder les lignes des utilisateurs choisis, ajouter cle_musique, séparer selon la date

nb_hdfs = 0
lignes_kafka = []

with open(FICHIER_SOURCE, encoding="utf-8") as f, \
     open(FICHIER_HDFS, "w", encoding="utf-8", newline="\n") as sortie_hdfs:
    for ligne in f:
        champs = ligne.rstrip("\n").split("\t")
        if len(champs) != 6:             # sécurité : ligne mal formée
            continue
        if champs[0] not in choisis:     # utilisateur non retenu
            continue

        # champs[3] = nom_artiste, champs[5] = nom_musique
        cle_musique = normaliser(champs[3]) + "||" + normaliser(champs[5])
        nouvelle_ligne = "\t".join(champs + [cle_musique]) + "\n"

        # champs[1] = horodatage, au format "2009-05-04T23:08:57Z"
        if champs[1] < DATE_COUPURE:
            sortie_hdfs.write(nouvelle_ligne)
            nb_hdfs += 1
        else:
            lignes_kafka.append(nouvelle_ligne)

# ecoutes doivent être envoyées a kafka dans l'ordre chronologique
lignes_kafka.sort(key=lambda l: l.split("\t")[1])

with open(FICHIER_KAFKA, "w", encoding="utf-8", newline="\n") as sortie_kafka:
    sortie_kafka.writelines(lignes_kafka)

print(f"Lignes écrites dans {FICHIER_HDFS} : {nb_hdfs}")
print(f"Lignes écrites dans {FICHIER_KAFKA} : {len(lignes_kafka)}")