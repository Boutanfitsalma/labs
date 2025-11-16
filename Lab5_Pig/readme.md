# Lab 5 - Apache Pig


## 🗂️ Structure du projet

```
Lab5_Pig/
├── pig_scripts/                  # Scripts Pig Latin
│   ├── wordcount.pig             # Exemple WordCount (✅ Terminé)
│   ├── employees.pig             # Analyse des employés (✅ Terminé)
│   ├── flights.pig               # Analyse des vols (✅ Terminé)
│   ├── films.pig                 # Analyse des films (✅ Terminé)
│   └── convert_json_to_csv.py    # Script de conversion JSON→CSV
├── data/                         # Données sources
│   ├── alice.txt
│   ├── employees.txt
│   ├── departments.txt
│   ├── films.json / films.csv
│   ├── artists.json / artists.csv
│   ├── film_actors.csv
│   └── flights/
│       └── sample_flights.csv
├── screenshots/                  # Captures d'écran des résultats
│   ├── 01_wordcount.png
│   ├── 02_employees.png
│   ├── 03_flights.png
│   └── 04_films.png
├── docs/
│   └── lab5_APACHE_PIG.pdf       # Énoncé du TP
└── README.md
```


## 🔧 Installation Apache Pig

Dans le conteneur `hadoop-master` :

```bash
# Télécharger et installer Pig
wget https://dlcdn.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz
tar -zxvf pig-0.17.0.tar.gz
mv pig-0.17.0 /usr/local/pig
rm pig-0.17.0.tar.gz

# Configurer les variables d'environnement
echo 'export PIG_HOME=/usr/local/pig' >> ~/.bashrc
echo 'export PATH=$PATH:$PIG_HOME/bin' >> ~/.bashrc
source ~/.bashrc

# Démarrer Hadoop et les services
./start-hadoop.sh
yarn timelineserver &
mapred --daemon start historyserver
```

## 📝 Scripts réalisés

### 1. WordCount (`wordcount.pig`) ✅

Compte les occurrences de chaque mot dans le texte d'Alice au Pays des Merveilles.

**Exécution :**
```bash
# Mode local
pig -x local
grunt> exec /shared_volume/wordcount.pig
```

**Résultats :**
- Fichier d'entrée : `alice.txt` (15 lignes)
- Sortie : `/shared_volume/pig_out/WORD_COUNT/`
- 119 mots uniques identifiés
- **Screenshot** : `screenshots/01_wordcount.png`

---

### 2. Analyse des employés (`employees.pig`) ✅

Analyse complète des données d'employés d'une entreprise.

**Préparation des données :**
```bash
# Copier les données sur HDFS
hdfs dfs -mkdir -p /input/employees
hdfs dfs -put /shared_volume/employees.txt /input/employees/
hdfs dfs -put /shared_volume/departments.txt /input/employees/
```

**Exécution :**
```bash
pig /shared_volume/employees.pig
```

**Analyses effectuées :**
1. ✅ Salaire moyen par département
2. ✅ Nombre d'employés par département
3. ✅ Liste des employés avec leurs départements
4. ✅ Employés avec salaire > 60 000€
5. ✅ Département avec le salaire moyen le plus élevé
6. ✅ Départements sans employés
7. ✅ Nombre total d'employés dans l'entreprise
8. ✅ Employés de la ville de Paris
9. ✅ Salaire total par ville
10. ✅ Départements ayant des femmes employées (heuristique sur prénoms)

**Résultats :**
- 20 employés analysés
- 6 départements
- Sortie finale : `/pigout/employes_femmes/` (4 départements)
- **Screenshot** : `screenshots/02_employees.png`

---

### 3. Analyse des vols aériens (`flights.pig`) ✅

Traitement et analyse de données de vols commerciaux.

**Préparation :**
```bash
hdfs dfs -mkdir -p /input/flights
hdfs dfs -put /shared_volume/sample_flights.csv /input/flights/
```

**Analyses réalisées :**
- Top 20 aéroports par volume total de vols (arrivées + départs)
- Popularité des transporteurs (volume logarithmique par année)
- Proportion de vols retardés (retard > 15 min) par année
- Retards par transporteur et par année
- Itinéraires les plus fréquentés

**Résultats :**
- Sortie : `/pigout/top_routes/`
- **Screenshot** : `screenshots/03_flights.png`

---

### 4. Analyse des films (`films.pig`) ✅

Traitement de données cinématographiques (films, réalisateurs, acteurs).

#### 📌 Note importante sur le traitement JSON

L'énoncé demandait de traiter directement les fichiers JSON avec `JsonLoader`. 
Cependant, nous avons rencontré les limitations suivantes :

- ❌ `JsonLoader` ne supporte pas les noms de champs commençant par `_` (comme `_id`)
- ❌ Le format JSON pretty-printed n'est pas compatible avec `JsonLoader` (qui attend du JSON Lines)
- ❌ La bibliothèque Piggybank présentait des bugs de compatibilité

**Solution adoptée** : Conversion JSON → CSV via script Python (`convert_json_to_csv.py`), puis traitement avec `PigStorage`.

Cette approche est courante en production Big Data lorsque les données sources ne sont pas dans le format optimal pour l'outil de traitement.

**Préparation :**
```bash
# 1. Convertir JSON en CSV (sur Windows)
python pig_scripts/convert_json_to_csv.py

# 2. Copier vers HDFS
hdfs dfs -mkdir -p /input/films
hdfs dfs -put /shared_volume/films.csv /input/films/
hdfs dfs -put /shared_volume/artists.csv /input/films/
hdfs dfs -put /shared_volume/film_actors.csv /input/films/
```

**Analyses effectuées :**
1. Films américains groupés par année
2. Films américains groupés par réalisateur
3. Extraction des acteurs (triplets film-acteur-rôle)
4. Jointure films + acteurs avec informations complètes
5. Films complets avec tous leurs acteurs (COGROUP)
6. Acteurs/Réalisateurs : nombre de films joués ET réalisés par artiste

## 📸 Captures d'écran

Toutes les captures d'écran des résultats sont disponibles dans le dossier `screenshots/`.

