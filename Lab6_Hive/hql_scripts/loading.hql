-- ========================================
-- Lab 6: Apache Hive - Script de Chargement
-- ========================================
-- Auteur: Votre Nom
-- Date: 16/11/2025
-- Description: Chargement des données dans les tables

-- Se connecter à la base de données
USE hotel_booking;

-- 1. Charger les données dans la table clients
LOAD DATA LOCAL INPATH '/shared_volume/clients.txt' INTO TABLE clients;

-- 2. Charger les données dans la table hotels
LOAD DATA LOCAL INPATH '/shared_volume/hotels.txt' INTO TABLE hotels;

-- 3. Charger les données dans la table reservations
LOAD DATA LOCAL INPATH '/shared_volume/reservations.txt' INTO TABLE reservations;

-- 4. Charger les données dans hotels_partitioned avec partition dynamique
INSERT OVERWRITE TABLE hotels_partitioned PARTITION(ville)
SELECT hotel_id, nom, etoiles, ville FROM hotels;

-- 5. Charger les données dans reservations_bucketed
INSERT OVERWRITE TABLE reservations_bucketed
SELECT * FROM reservations;

-- 6. Vérifier le chargement des données
SELECT 'Clients:' AS table_name, COUNT(*) AS count FROM clients
UNION ALL
SELECT 'Hotels:', COUNT(*) FROM hotels
UNION ALL
SELECT 'Reservations:', COUNT(*) FROM reservations
UNION ALL
SELECT 'Hotels Partitioned:', COUNT(*) FROM hotels_partitioned
UNION ALL
SELECT 'Reservations Bucketed:', COUNT(*) FROM reservations_bucketed;

-- 7. Aperçu des données
SELECT * FROM clients LIMIT 5;
SELECT * FROM hotels LIMIT 5;
SELECT * FROM reservations LIMIT 5;
