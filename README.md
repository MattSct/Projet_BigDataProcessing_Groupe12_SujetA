# Projet Big Data Processing – Groupe 12 – Sujet A

Pipeline Big Data de bout en bout appliqué à une plateforme de **streaming musical** (adaptation du sujet A : catalogue, historique, flux en direct et recommandations personnalisées).

**Équipe :** Adrien GARDETTE, Adrien PICARD, Matthieu SA COUTINHO
**Module :** Big Data Processing – ECE Paris

## Le projet en bref

On part du dataset public Last.fm 1K (écoutes réelles de ~1 000 utilisateurs entre 2005 et 2009). On en tire 100 utilisateurs, puis on coupe les données au 1er mai 2009 :
- tout ce qui est **avant** la coupure devient l'historique, archivé dans HDFS et traité en batch ;
- tout ce qui est **après** est rejoué dans Kafka pour simuler des écoutes en temps réel.

L'architecture suit le modèle **Lambda** : une couche batch (HDFS + Spark + MLlib), une couche temps réel (Kafka + Spark Structured Streaming) et une couche service commune (MongoDB). Tout tourne en local dans Docker, sur un réseau commun.

Le schéma complet est fourni à part (`Schema_Architecture_Groupe12_SujetA`).

## Organisation du code par brique

| Brique | Rôle | Fichiers |
|---|---|---|
| 0. Données | Tirage des 100 utilisateurs et coupure historique / flux | `projet/tirage_au_sort.py` |
| 1. HDFS | Stockage de l'historique brut (archive) | `docker-compose.yml`, `hadoop.env` (commandes ci-dessous) |
| 2. MongoDB | Catalogue + collection courante (CRUD) | `mongo.yml`, `projet/generer_catalogue.py`, `projet/catalogue.json`, `projet/crud_playlists.js` |
| 3. Spark batch | Statistiques historiques (top artistes, top musiques, écoutes par heure) | `projet/spark_batch.py` |
| 4. Kafka | Ingestion du flux d'écoutes (topic `ecoutes`) | `kafka.yml`, `projet/producteur_ecoutes.py` |
| 5. Spark Streaming | Agrégation sur fenêtres glissantes, écriture dans MongoDB | `projet/spark_streaming.py` |
| 6. Spark MLlib | Recommandations par filtrage collaboratif (ALS) | `projet/spark_als.py` |

Autres fichiers :
- `spark.yml` et `spark/Dockerfile` : image Jupyter/PySpark 3.5.3 avec `pymongo` et `kafka-python` installés
- `projet/klaxons.py` : petit script d'exploration utilisé dans le rapport (poids d'un seul utilisateur sur un artiste)
- les dossiers `base/`, `namenode/`, `datanode/`, `nginx/`, `submit/`... viennent du dépôt d'origine [big-data-europe/docker-hadoop](https://github.com/big-data-europe/docker-hadoop) sur lequel on s'est appuyé

## Prérequis

- Docker Desktop (testé sous Windows avec WSL2)
- environ 8 Go de RAM : sur une machine juste, on peut couper les services YARN (resourcemanager, nodemanager, historyserver), le pipeline ne les utilise pas

Toutes les commandes ci-dessous se lancent depuis la racine du dépôt (PowerShell). Les scripts Python tournent **dans le conteneur spark**, qui voit le dossier `projet/` sous `/home/jovyan/work`.

## Récupérer les données

`ecoutes_historique.tsv` (244 Mo) n'est pas dans le dépôt car il dépasse la limite de GitHub. Il se régénère à l'identique :

1. Télécharger le fichier `userid-timestamp-artid-artname-traid-traname.tsv` du dataset Last.fm 1K (par exemple depuis les releases de [eifuentes/lastfm-dataset-1K](https://github.com/eifuentes/lastfm-dataset-1K)) et le placer dans `projet/`
2. Lancer le tirage (graine fixée à 42, donc toujours le même résultat) :
   ```
   docker exec -w /home/jovyan/work spark python tirage_au_sort.py
   ```
3. Vérifier l'empreinte :
   ```
   Get-FileHash projet\ecoutes_historique.tsv -Algorithm SHA256
   ```
   Résultat attendu : `E1215BAFB585C5200AE433894085261FFC149FFC3F98F0763AAE573CC75EF8F9`

`ecoutes_flux.tsv` (24 762 écoutes) et `catalogue.json` (242 478 musiques) sont déjà fournis.

## Lancer le pipeline

### 1. Démarrer l'environnement

```
docker compose up -d --build
docker ps
```

Le service `kafka-init` crée automatiquement le topic `ecoutes` (3 partitions) puis s'arrête.

### 2. Déposer l'historique dans HDFS

```
docker cp projet/ecoutes_historique.tsv namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /data/lastfm/raw
docker exec namenode hdfs dfs -put -f /tmp/ecoutes_historique.tsv /data/lastfm/raw/
docker exec namenode bash -c "hdfs dfs -cat /data/lastfm/raw/ecoutes_historique.tsv | wc -l"
```

On doit obtenir 1 530 543 lignes.

### 3. Importer le catalogue et lancer le CRUD MongoDB

```
docker cp projet/catalogue.json mongo:/tmp/catalogue.json
docker exec mongo mongoimport --db streaming_musique --collection catalogue --jsonArray --drop --file /tmp/catalogue.json

docker cp projet/crud_playlists.js mongo:/tmp/
docker exec mongo mongosh streaming_musique --file /tmp/crud_playlists.js
```

### 4. Traitements batch (statistiques + recommandations)

```
docker exec -it spark spark-submit --driver-memory 1g --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 /home/jovyan/work/spark_batch.py
docker exec -it spark spark-submit --driver-memory 1g /home/jovyan/work/spark_als.py
```

### 5. Temps réel

Dans un premier terminal, démarrer le streaming (il lit le topic en `latest`, donc il faut le lancer **avant** le producteur) :

```
docker exec -it spark spark-submit --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /home/jovyan/work/spark_streaming.py
```

Dans un second terminal, rejouer le flux (20 messages/s, `--limite` pour un test court) :

```
docker exec -it spark python /home/jovyan/work/producteur_ecoutes.py --limite 3600
```

Pour voir les messages passer dans Kafka :

```
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ecoutes --max-messages 5 --property print.key=true
```

### 6. Vérifier les résultats

```
docker exec mongo mongosh streaming_musique --quiet --eval "db.getCollectionNames().forEach(c => print(c, db[c].countDocuments()))"
```

## Les contrats entre briques

**Contrat 1 – HDFS :** `hdfs://namenode:9000/data/lastfm/raw/ecoutes_historique.tsv`, TSV sans en-tête, 7 colonnes : `user_id`, `horodatage`, `id_artiste`, `nom_artiste`, `id_musique`, `nom_musique`, `cle_musique` (artiste et titre normalisés, séparés par `||`).

**Contrat 2 – Kafka :** topic `ecoutes` sur `kafka:9092`, clé = `user_id`, valeur = JSON UTF-8 :
```json
{"user_id": "user_000651", "horodatage_origine": "2009-05-01T00:00:46Z",
 "horodatage_envoi": "2026-10-02T14:03:11Z", "nom_artiste": "Kate Ryan",
 "nom_musique": "Your Eyes", "cle_musique": "kate ryan||your eyes"}
```

**Contrat 3 – MongoDB :** base `streaming_musique`

| Collection | Écrite par | Contenu |
|---|---|---|
| `catalogue` | mongoimport | une musique distincte par document |
| `playlists` | CRUD mongosh | collection courante de l'application |
| `stats_historiques` | `spark_batch.py` | top artistes, top musiques, écoutes par heure |
| `agregats_temps_reel` | `spark_streaming.py` | nombre d'écoutes par musique et par fenêtre de 5 min |
| `recommandations` | `spark_als.py` | top 10 par utilisateur, avec score et date de calcul |

## Quelques choix techniques

- **Lecture HDFS directe depuis Spark** (`hdfs://namenode:9000`) : possible ici parce que Spark tourne dans le même réseau Docker que le cluster Hadoop, ce qui évite de passer par une copie locale.
- **Écriture MongoDB :** connecteur Spark-MongoDB pour le batch, pymongo pour le streaming (dans `foreachBatch`, comme recommandé par le sujet) et pour l'ALS (100 utilisateurs, donc un `collect()` reste léger).
- **Streaming :** fenêtres de 5 min glissant chaque minute sur `horodatage_envoi`, watermark de 1 min, mode `update` avec upsert dans MongoDB pour ne pas dupliquer une fenêtre mise à jour, checkpoint pour reprendre après un arrêt.
- **ALS :** mode implicite, note = `log(1 + nombre d'écoutes)` pour limiter le poids des très gros auditeurs, uniquement les musiques écoutées par au moins 2 utilisateurs, et les titres déjà écoutés sont retirés des recommandations.
