# Séance 2 — Airflow : exercices corrigés

**Module :** Données de santé  
**Étudiant :** Qais Kazimi  
**Formation :** Option Data 2026

## Présentation

Ce dossier contient les exercices réalisés lors de la séance 2 consacrée à Apache Airflow.

L'objectif de cette séance est de comprendre l'orchestration des pipelines de données, la planification des tâches et le traitement quotidien des données de santé au format FHIR.

## Contenu du dossier

| Fichier | Description |
|---|---|
| `mon_premier_dag.py` | Création d'un premier DAG avec les tâches extraire, transformer, contrôler et charger. |
| `prevoir_executions.py` | Planification quotidienne d'un DAG avec `catchup=True`. |
| `prevoir_sans_executions.py` | Planification quotidienne d'un DAG avec `catchup=False`. |
| `fhir_glycemies_quotidien.py` | Pipeline de traitement des glycémies : extraction, transformation en Parquet, validation avec Pandera et chargement dans DuckDB. |

## Technologies utilisées

- Python
- Apache Airflow
- Docker
- FHIR
- Pandas et Parquet
- Pandera
- DuckDB

## Résultats

Le pipeline FHIR a été testé avec les données d'exemple fournies pendant le TD.

- 684 observations lues.
- 664 observations valides.
- 20 observations rejetées.
- 664 glycémies chargées dans DuckDB.

Un deuxième lancement a permis de vérifier l'absence de doublons.

**Remarque :** Les tests ont été effectués avec `source=fichier`. L'extraction depuis l'API FHIR reste à compléter.

## Conclusion

Cette séance m'a permis de découvrir le fonctionnement d'Apache Airflow, la création de DAGs, la gestion des dépendances entre les tâches et la construction d'un pipeline de données de santé.