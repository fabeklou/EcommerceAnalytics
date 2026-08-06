# Ecommerce Analytics Pipeline - Spark & Scala

## 📌 Présentation du Projet
Ce projet consiste en une plateforme d'analyse de données e-commerce développée avec **Apache Spark 3.5** et **Scala 2.13**.
L'objectif est de traiter des flux de données multi-formats (CSV, JSON, Parquet), de les valider, de les enrichir via des fonctions avancées (UDF, Window Functions) et de générer des indicateurs métiers (KPIs) exploitables.

### Points clés de l'implémentation :
*   **Architecture Modulaire** : Séparation stricte entre les modèles de données, la logique d'ingestion et les transformations.
*   **Sécurité du Typage** : Utilisation intensive des `Datasets[T]` et des `case classes` pour garantir la robustesse du code.
*   **Configuration Externalisée** : Gestion des paramètres via la bibliothèque Typesafe Config (HOCON).
*   **Optimisation Spark** : Mise en œuvre du `Caching`, `Broadcasting` et gestion fine des partitions.

---

## 🛠️ Prérequis
Pour exécuter ce projet localement ou sur un cluster, assurez-vous d'avoir :
*   **Java Development Kit (JDK)** : Version 11 (recommandée pour Spark 3.x).
*   **Scala** : Version 2.13.12.
*   **SBT (Simple Build Tool)** : Version 1.9.x ou supérieure.
*   **Apache Spark** : Version 3.5.x (nécessaire pour l'exécution via `spark-submit`).

---

## 🏗️ Compilation et Build
Le projet utilise SBT pour la gestion des dépendances et la génération de l'artefact.

1. **Nettoyer et compiler le projet :**
   ```bash
   sbt clean compile
   ```
2. **Générer le fichier JAR (pour le déploiement) :**
   ```bash
   sbt package
   ```
   *Le fichier généré se trouvera dans : `target/scala-2.13/ecommerceanalytics_2.13-0.1.0-SNAPSHOT.jar`*

---

## 🚀 Exécution

### 1. Mode Développement (Local via SBT)
Idéal pour tester rapidement vos modifications durant la phase de dev :
```bash
sbt run
```
*SBT chargera automatiquement les dépendances et exécutera la méthode `MainApp`.*

### 2. Mode Production (Déploiement via Spark-submit)
Pour exécuter l'application sur un cluster (ou en local de manière isolée) :
```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  --driver-memory 2G \
  --executor-memory 2G \
  target/scala-2.13/ecommerceanalytics_2.13-0.1.0-SNAPSHOT.jar
```

---

## 📂 Structure du Projet
```text
EcommerceAnalytics/
├── src/main/scala/
│   ├── com.ecommerce.analytics/   # Logique métier (Ingestion, Transformation)
│   └── com.ecommerce.models/      # Définition des schémas (Case Classes)
├── src/main/resources/
│   ├── data/                      # Landing zone des fichiers sources
│   └── application.conf           # Configuration externalisée
├── build.sbt                      # Gestion des dépendances Spark & Scala
└── README.md                      # Documentation
```

---

## 📈 Fonctionnalités Implémentées
- [x] Ingestion multi-sources (CSV, JSON, Parquet).
- [x] Validation des données (Data Quality Rules).
- [x] Enrichissement temporel (UDF avancée).
- [x] Analyse de cohortes et KPIs marchands.
- [x] Persistance des résultats en formats optimisés.

## Auteur

- **Fabrice EKLOU**  &mdash;  (data | ML) Engineer:
    - [LinkedIn](https://www.linkedin.com/in/fabeklou/)
    - [GitHub](https://github.com/fabeklou)

---