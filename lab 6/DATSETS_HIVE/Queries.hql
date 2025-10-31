USE hotel_booking;

-- Lister tous les clients
SELECT * FROM clients;

-- Lister tous les hôtels à Paris
SELECT * FROM hotels WHERE ville = 'Paris';

-- Lister toutes les réservations avec infos hôtels et clients
SELECT r.*, h.nom AS hotel_nom, c.nom AS client_nom
FROM reservations r
JOIN hotels h ON r.hotel_id = h.hotel_id
JOIN clients c ON r.client_id = c.client_id;

-- Nombre de réservations par client
SELECT client_id, COUNT(*) AS nb_reservations
FROM reservations
GROUP BY client_id;

-- Clients ayant réservé plus de 2 nuitées
SELECT client_id
FROM reservations
GROUP BY client_id
HAVING COUNT(*) > 2;

-- Hôtels réservés par chaque client
SELECT r.client_id, h.nom AS hotel_nom
FROM reservations r
JOIN hotels h ON r.hotel_id = h.hotel_id;

-- Noms des hôtels avec plus d'une réservation
SELECT h.nom
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom
HAVING COUNT(r.reservation_id) > 1;

-- Noms des hôtels sans réservation
SELECT h.nom
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;

-- Clients ayant réservé un hôtel avec plus de 4 étoiles
SELECT DISTINCT c.*
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
WHERE h.etoiles > 4;

-- Total des revenus générés par chaque hôtel
SELECT h.hotel_id, h.nom, SUM(r.prix_total) AS total_revenu
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.nom;

-- Revenus totaux par ville (partitionnée)
SELECT ville, SUM(prix_total) AS total_revenu
FROM hotels_partitioned h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY ville;

-- Nombre total de réservations par client (bucketed)
SELECT client_id, COUNT(*) AS nb_reservations
FROM reservations_bucketed
GROUP BY client_id;

-- Nettoyage : supprimer les tables
DROP TABLE IF EXISTS clients;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS hotels_partitioned;
DROP TABLE IF EXISTS reservations_bucketed;