-- ========================================
-- Lab 6: Apache Hive - Script de Création
-- ========================================
-- Auteur: Votre Nom
-- Date: 16/11/2025
-- Description: Création de la base de données et des tables

-- 1. Créer la base de données
CREATE DATABASE IF NOT EXISTS hotel_booking;
USE hotel_booking;

-- 2. Activer les propriétés pour partitions et buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.enforce.bucketing=true;

-- 3. Créer la table clients
CREATE TABLE IF NOT EXISTS clients (
    client_id INT,
    nom STRING,
    email STRING,
    telephone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 4. Créer la table hotels
CREATE TABLE IF NOT EXISTS hotels (
    hotel_id INT,
    nom STRING,
    ville STRING,
    etoiles INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 5. Créer la table reservations
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut DATE,
    date_fin DATE,
    prix_total DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 6. Créer la table hotels_partitioned (partitionnée par ville)
CREATE TABLE IF NOT EXISTS hotels_partitioned (
    hotel_id INT,
    nom STRING,
    etoiles INT
)
PARTITIONED BY (ville STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 7. Créer la table reservations_bucketed (bucketed par client_id)
CREATE TABLE IF NOT EXISTS reservations_bucketed (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut DATE,
    date_fin DATE,
    prix_total DECIMAL(10,2)
)
CLUSTERED BY (client_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 8. Vérifier les tables créées
SHOW TABLES;
