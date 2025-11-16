\# Lab 4 -- HBase & Spark

\## Objectifs du TP

1\. Charger un dataset dans HDFS 2. Créer et alimenter une table HBase
3. Interroger et vérifier les données 4. Intégrer Spark pour analyser
les données distribuées

\-\--

\## 📂 Structure du projet

\`\`\` Lab4_HBase/ │ ├── HelloHBase.java \# Code Java pour HBase
(création + insertion + lecture) ├── purchases_2.txt \# Dataset (NON
inclus GitHub car \>100MB) ├── Screenshots/ \# Captures d'écran de
l'exécution └── hbase_spark/ ├── HbaseSparkProcess.java └──
HbaseSparkAnalytics.java \`\`\`

\-\--

\## Partie 1 --- HBase

\### Dataset utilisé

\`\`\` purchases_2.txt \`\`\`

⚠ \*\*Non ajouté à GitHub\*\* (taille : 232MB, limite GitHub = 100MB)

\-\--

\## Étapes réalisées

\### ✔ 1. Import du dataset dans Docker

Le fichier \`purchases_2.txt\` a été copié dans le conteneur via un
volume Docker.

\### ✔ 2. Création de la table HBase

Commande exécutée dans le shell HBase :

\`\`\`hbase create \'products\', \'cf\' \`\`\`

\### ✔ 3. Insertion et lecture via Java

Le fichier \`HelloHBase.java\` effectue :

\* Connexion au cluster HBase \* Création de table (si inexistante) \*
Insertion de plusieurs lignes d'exemple \* Lecture et affichage des
données

Fichier : \`Lab4_HBase/HelloHBase.java\`

\-\--

\## Partie 2 --- Spark + HBase

\*\*Objectif :\*\* lire les données stockées dans la table HBase et
effectuer des analyses distribuées.

\### Scripts inclus

\#### HbaseSparkProcess.java

Lit la table HBase \`products\` via Spark :

\* Configuration HBase \* Utilisation de \`TableInputFormat\` \*
Comptage des lignes de la table

Fichier : \`Lab4_HBase/hbase_spark/HbaseSparkProcess.java\`

\-\--

\#### HbaseSparkAnalytics.java

Script Spark complet permettant :

\* Somme totale de toutes les ventes \* Total des ventes par produit \*
Total des ventes par ville \* Statistiques globales :

\* min \* max \* moyenne

Fichier : \`Lab4_HBase/hbase_spark/HbaseSparkAnalytics.java\`

\-\--

\## Résultats obtenus

\### Avec HBase :

\* Table \`products\` créée et correctement alimentée \* Lectures OK \*
Vérification de la présence des lignes : success

\### Avec Spark :

\* RDD créé depuis HBase \* Nombre total de lignes affiché \* Calculs
distribués effectués :

\* Total ventes = OK \* Ventes par produit = OK \* Ventes par ville = OK
\* Statistiques globales = OK

\-\--

\## Conclusion

Ce TP a permis de :

\* Manipuler un dataset volumineux dans HDFS \* Gérer des données NoSQL
avec HBase \* Intégrer Spark pour analyser des données distribuées \*
Développer des programmes Java pour interagir avec HBase et Spark

\-\--
