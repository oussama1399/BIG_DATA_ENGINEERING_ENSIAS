USE hotel_booking;

-- Charger les données dans les tables
LOAD DATA LOCAL INPATH '/shared_volume/clients.txt' INTO TABLE clients;
LOAD DATA LOCAL INPATH '/shared_volume/hotels.txt' INTO TABLE hotels;

-- Charger les données dans reservations (partition statique, à adapter selon tes données)
LOAD DATA LOCAL INPATH '/shared_volume/reservations.txt' INTO TABLE reservations PARTITION (date_debut='2025-10-31');

-- Charger les données dans hotels_partitioned (exemple pour Paris, à adapter)
LOAD DATA LOCAL INPATH '/shared_volume/hotels.txt' INTO TABLE hotels_partitioned PARTITION (ville='Paris');

-- Charger les données dans reservations_bucketed via INSERT
INSERT INTO TABLE reservations_bucketed
SELECT reservation_id, client_id, hotel_id, CAST(date_debut AS DATE), CAST(date_fin AS DATE), prix_total
FROM reservations;