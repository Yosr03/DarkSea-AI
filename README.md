# DarkSea-AI — Maritime Intelligence Platform

## Présentation du projet

DarkSea-AI est une plateforme Big Data intégrant l'intelligence artificielle pour analyser les interruptions du signal AIS et détecter les comportements maritimes atypiques.

Le projet combine le traitement distribué, l'analyse historique, le Machine Learning, le streaming et la visualisation interactive.

## Architecture technique

- **Hadoop HDFS** : stockage distribué des données.
- **Apache Pig** : extraction de nouvelles connaissances.
- **Apache Hive** : organisation et exploitation des tables analytiques.
- **Apache Spark** : traitement distribué des données.
- **Spark MLlib** : détection d'anomalies avec KMeans.
- **Apache Kafka** : simulation de flux en temps réel.
- **Spark Structured Streaming** : analyse dynamique et génération d'alertes.
- **Streamlit** : dashboard métier interactif.
- **Grafana et Prometheus** : supervision technique prévue.
- **Docker Compose** : orchestration des services.

## Dataset

Source : Global Fishing Watch.

- 55 368 événements d'interruption AIS.
- 5 269 profils de navires.
- Période historique : 2017–2019.

## Résultats obtenus

| Indicateur | Résultat |
|---|---:|
| Tables Hive | 14 |
| Cellules géographiques | 994 |
| Profils atypiques | 327 |
| Événements historiques atypiques | 2 742 |
| Événements rejoués via Kafka | 11 123 |
| Débit du producteur | 50 événements/s |
| Alertes générées | 551 |
| Concordance Batch / Streaming | 100 % |
| Doublons | 0 |
| Événements manquants | 0 |
| Latence moyenne jusqu'au scoring | 7,71 s |
| Latence P95 jusqu'au scoring | 21,44 s |

## Organisation du projet

- `docker/` : infrastructure et configuration des services.
- `pig/` : scripts d'extraction de connaissances.
- `hive/` : requêtes et tables analytiques.
- `spark/` : Machine Learning, traitement distribué et validation.
- `streaming/` : production des événements Kafka.
- `dashboard/` : interface Streamlit et tests.
- `monitoring/` : future supervision Grafana et Prometheus.

## État d'avancement

- [x] Infrastructure Big Data
- [x] Intégration Hadoop, Pig, Hive et Spark
- [x] Extraction des connaissances
- [x] Entraînement des modèles KMeans
- [x] Validation des modèles
- [x] Intégration Kafka
- [x] Spark Structured Streaming
- [x] Validation Batch / Streaming
- [x] Première page du dashboard
- [ ] Finalisation du dashboard interactif
- [ ] Benchmark Pandas vs Spark
- [ ] Grafana et Prometheus
- [ ] Optimisation des performances
- [ ] Tests de robustesse
- [ ] Documentation complète d'installation

## Limites

Les anomalies détectées constituent des signaux à examiner et non des preuves d'activités illégales.

Le streaming repose sur une simulation utilisant des événements historiques, et non sur un flux AIS maritime en direct.

Les datasets, modèles générés et exports analytiques ne sont pas inclus dans le dépôt Git. Ils doivent être récupérés ou régénérés avant l'exécution complète du projet.

## Contexte académique

Projet de Big Data Avancé — réalisé en équipe de trois étudiants.