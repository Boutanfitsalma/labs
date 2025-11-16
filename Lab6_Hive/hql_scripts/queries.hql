-- ========================================
-- Lab 6: Apache Hive - Script de Requêtes
-- ========================================
-- Auteur: Votre Nom
-- Date: 16/11/2025
-- Description: Requêtes analytiques sur les données de réservation

-- Se connecter à la base de données
USE hotel_booking;

-- ========================================
-- SECTION 5: Requêtes Simples
-- ========================================

-- 1. Lister tous les clients
SELECT * FROM clients;

-- 2. Lister tous les hôtels à Paris
SELECT * FROM hotels WHERE ville = 'Paris';

-- 3. Lister toutes les réservations avec informations hôtels et clients
SELECT 
    r.reservation_id,
    c.nom AS client_nom,
    h.nom AS hotel_nom,
    h.ville,
    r.date_debut,
    r.date_fin,
    r.prix_total
FROM reservations r
JOIN clients c ON r.client_id = c.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
LIMIT 20;

-- ========================================
-- SECTION 6: Requêtes avec Jointures
-- ========================================

-- 1. Afficher le nombre de réservations par client
SELECT 
    c.nom,
    COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
LEFT JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.nom
ORDER BY nombre_reservations DESC;

-- 2. Afficher les clients qui ont réservé plus que 2 nuitées
SELECT 
    c.nom,
    COUNT(r.reservation_id) AS nombre_reservations,
    SUM(DATEDIFF(r.date_fin, r.date_debut)) AS total_nuitees
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.nom
HAVING SUM(DATEDIFF(r.date_fin, r.date_debut)) > 2
ORDER BY total_nuitees DESC;

-- 3. Afficher les Hôtels réservés par chaque client
SELECT 
    c.nom AS client_nom,
    h.nom AS hotel_nom,
    h.ville,
    h.etoiles
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
ORDER BY c.nom, h.nom;

-- 4. Afficher les noms des hôtels dans lesquels il y a plus qu'une réservation
SELECT 
    h.nom AS hotel_nom,
    h.ville,
    COUNT(r.reservation_id) AS nombre_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom, h.ville
HAVING COUNT(r.reservation_id) > 1
ORDER BY nombre_reservations DESC;

-- 5. Afficher les noms des hôtels dans lesquels il n'y a pas de réservation
SELECT 
    h.nom AS hotel_nom,
    h.ville,
    h.etoiles
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;

-- ========================================
-- SECTION 7: Requêtes Imbriquées
-- ========================================

-- 1. Afficher les clients ayant réservé un hôtel avec plus de 4 étoiles
SELECT DISTINCT
    c.nom AS client_nom,
    c.email
FROM clients c
WHERE c.client_id IN (
    SELECT r.client_id
    FROM reservations r
    JOIN hotels h ON r.hotel_id = h.hotel_id
    WHERE h.etoiles > 4
)
ORDER BY c.nom;

-- 2. Afficher le Total des revenus générés par chaque hôtel
SELECT 
    h.nom AS hotel_nom,
    h.ville,
    h.etoiles,
    COALESCE(SUM(r.prix_total), 0) AS revenus_total
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom, h.ville, h.etoiles
ORDER BY revenus_total DESC;

-- ========================================
-- SECTION 8: Agrégations avec Partitions et Buckets
-- ========================================

-- 1. Revenus totaux par ville (table partitionnée)
SELECT 
    hp.ville,
    COUNT(DISTINCT hp.hotel_id) AS nombre_hotels,
    COUNT(r.reservation_id) AS nombre_reservations,
    SUM(r.prix_total) AS revenus_total
FROM hotels_partitioned hp
JOIN reservations r ON hp.hotel_id = r.hotel_id
GROUP BY hp.ville
ORDER BY revenus_total DESC;

-- 2. Nombre total de réservations par client (table bucketed)
SELECT 
    rb.client_id,
    c.nom AS client_nom,
    COUNT(rb.reservation_id) AS nombre_reservations,
    SUM(rb.prix_total) AS total_depense,
    AVG(rb.prix_total) AS depense_moyenne
FROM reservations_bucketed rb
JOIN clients c ON rb.client_id = c.client_id
GROUP BY rb.client_id, c.nom
ORDER BY nombre_reservations DESC, total_depense DESC
LIMIT 10;

-- ========================================
-- ANALYSES COMPLÉMENTAIRES
-- ========================================

-- Statistiques générales
SELECT 
    'Total Clients' AS metrique, 
    COUNT(*) AS valeur 
FROM clients
UNION ALL
SELECT 
    'Total Hotels', 
    COUNT(*) 
FROM hotels
UNION ALL
SELECT 
    'Total Reservations', 
    COUNT(*) 
FROM reservations
UNION ALL
SELECT 
    'Revenu Total', 
    CAST(SUM(prix_total) AS INT)
FROM reservations;

-- Top 5 des hôtels les plus populaires
SELECT 
    h.nom,
    h.ville,
    h.etoiles,
    COUNT(r.reservation_id) AS nombre_reservations,
    SUM(r.prix_total) AS revenus
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom, h.ville, h.etoiles
ORDER BY nombre_reservations DESC
LIMIT 5;

-- Distribution des réservations par nombre d'étoiles
SELECT 
    h.etoiles,
    COUNT(r.reservation_id) AS nombre_reservations,
    SUM(r.prix_total) AS revenus_total,
    AVG(r.prix_total) AS prix_moyen
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.etoiles
ORDER BY h.etoiles DESC;
