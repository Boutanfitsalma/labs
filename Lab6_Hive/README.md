# Lab 6 : Apache Hive - Analyse de Réservations d'Hôtels 🏨

## 📋 Objectifs du Lab

Ce laboratoire vise à :
- ✅ Installer et configurer Apache Hive avec Docker
- ✅ Utiliser Beeline pour se connecter à HiveServer2
- ✅ Créer et gérer des bases de données et tables Hive
- ✅ Implémenter des partitions et buckets pour optimiser les performances
- ✅ Réaliser des requêtes analytiques complexes (jointures, agrégations, sous-requêtes)
- ✅ Travailler avec HDFS pour le stockage distribué

---

## 🛠️ Technologies Utilisées

- **Apache Hive** : 4.0.0-alpha-2
- **Docker** : Conteneurisation de l'environnement
- **HDFS** : Stockage distribué des données
- **Beeline** : Client JDBC pour Hive
- **HiveQL** : Langage de requêtes SQL-like

---

## 📂 Structure du Projet

```
Lab6_Hive/
├── shared_data/
│   ├── clients.txt           # 50 clients
│   ├── hotels.txt            # 20 hôtels dans 5 villes
│   └── reservations.txt      # 100 réservations
├── hql_scripts/
│   ├── creation.hql          # Création BD et tables
│   ├── loading.hql           # Chargement des données
│   └── queries.hql           # Requêtes analytiques
├── screenshots/   
└── README.md
```

---

## 🚀 Installation et Configuration

### 1. Pull l'image Docker Hive

```bash
docker pull apache/hive:4.0.0-alpha-2
```

### 2. Lancer le conteneur HiveServer2

```bash
docker run -v ~/path/to/Lab6_Hive/data:/shared_volume \
  -d -p 10000:10000 -p 10002:10002 -p 9083:9083 \
  --env SERVICE_NAME=hiveserver2 \
  --name hiveserver2-standalone \
  apache/hive:4.0.0-alpha-2
```

### 3. Accéder au conteneur

```bash
docker exec -it hiveserver2-standalone bash
```

### 4. Se connecter à Beeline

```bash
beeline -u jdbc:hive2://localhost:10000 scott tiger
```

### 5. Interface Web

Accéder à HiveServer2 via : http://localhost:10002

---

## 📊 Schéma de la Base de Données

### Tables Principales

#### **clients**
| Colonne     | Type   | Description              |
|-------------|--------|--------------------------|
| client_id   | INT    | Identifiant unique       |
| nom         | STRING | Nom complet du client    |
| email       | STRING | Email du client          |
| telephone   | STRING | Numéro de téléphone      |

#### **hotels**
| Colonne     | Type   | Description              |
|-------------|--------|--------------------------|
| hotel_id    | INT    | Identifiant unique       |
| nom         | STRING | Nom de l'hôtel           |
| ville       | STRING | Ville de l'hôtel         |
| etoiles     | INT    | Nombre d'étoiles (1-5)   |

#### **reservations**
| Colonne         | Type          | Description                    |
|-----------------|---------------|--------------------------------|
| reservation_id  | INT           | Identifiant unique             |
| client_id       | INT           | Référence au client            |
| hotel_id        | INT           | Référence à l'hôtel            |
| date_debut      | DATE          | Date de début de séjour        |
| date_fin        | DATE          | Date de fin de séjour          |
| prix_total      | DECIMAL(10,2) | Prix total de la réservation   |

### Tables Optimisées

#### **hotels_partitioned**
- **Partitionnée par** : `ville`
- **Avantage** : Améliore les performances des requêtes filtrant par ville

#### **reservations_bucketed**
- **Bucketed par** : `client_id` (4 buckets)
- **Avantage** : Optimise les jointures et les agrégations par client

---

## 🎯 Exécution des Scripts HQL

### Ordre d'exécution

```bash
# 1. Créer la base de données et les tables
beeline -u jdbc:hive2://localhost:10000 -f /path/to/creation.hql

# 2. Charger les données
beeline -u jdbc:hive2://localhost:10000 -f /path/to/loading.hql

# 3. Exécuter les requêtes analytiques
beeline -u jdbc:hive2://localhost:10000 -f /path/to/queries.hql
```

---

## 📈 Requêtes Analytiques Principales

### 1. Requêtes Simples
- Liste des clients
- Hôtels par ville
- Réservations avec détails

### 2. Jointures
- Nombre de réservations par client
- Clients avec plus de 2 nuitées
- Hôtels réservés par client
- Hôtels avec/sans réservations

### 3. Requêtes Imbriquées
- Clients ayant réservé des hôtels 4+ étoiles
- Revenus totaux par hôtel

### 4. Agrégations avec Partitions/Buckets
- Revenus par ville (table partitionnée)
- Top 10 clients (table bucketed)

---

## 🔍 Observations Importantes

### Structure HDFS Warehouse

Après création des tables, le warehouse HDFS contient :

```
/opt/hive/data/warehouse/hotel_booking.db/
├── clients/
├── hotels/
├── reservations/
├── hotels_partitioned/
│   ├── ville=Paris/
│   ├── ville=Lyon/
│   ├── ville=Marseille/
│   ├── ville=Nice/
│   └── ville=Toulouse/
└── reservations_bucketed/
    ├── 000000_0
    ├── 000001_0
    ├── 000002_0
    └── 000003_0
```

**Remarques** :
- Les **partitions** créent des sous-répertoires par valeur de clé (ville)
- Les **buckets** créent plusieurs fichiers (4 dans notre cas)
- Cela optimise les requêtes en évitant de scanner toutes les données

---

## 📊 Résultats Clés

### Statistiques Générales
- **50 clients** enregistrés
- **20 hôtels** répartis dans 5 villes
- **100 réservations** traitées
- **Revenu total** : ~48,000€

### Top Performances
- **Ville la plus lucrative** : Paris
- **Hôtel le plus réservé** : Grand Hotel Paris
- **Client le plus actif** : Dupont Jean (2 réservations)

---

## 🧹 Nettoyage

Pour supprimer toutes les tables et la base de données :

```sql
DROP TABLE IF EXISTS clients;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS hotels_partitioned;
DROP TABLE IF EXISTS reservations_bucketed;
DROP DATABASE IF EXISTS hotel_booking CASCADE;
```

---

## 📸 Screenshots

Les captures d'écran dans le dossier `screenshots/` documentent :
1. Interface Web HiveServer2
2. Connexion Beeline
3. Création de la base de données
4. Tables créées
5. Données chargées
6. Structure HDFS warehouse
7. Tables partitionnées
8. Résultats des requêtes


---
