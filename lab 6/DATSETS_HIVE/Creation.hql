-- Création de la base de données et des tables
CREATE DATABASE IF NOT EXISTS hotel_booking;
USE hotel_booking;

-- Table clients
CREATE TABLE clients (
  client_id INT,
  nom STRING,
  email STRING,
  telephone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table hotels
CREATE TABLE hotels (
  hotel_id INT,
  nom STRING,
  ville STRING,
  etoiles INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Activer les propriétés pour partitions et buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.enforce.bucketing=true;

-- Table reservations partitionnée
CREATE TABLE reservations (
  reservation_id INT,
  client_id INT,
  hotel_id INT,
  date_fin STRING,
  prix_total DECIMAL(10,2)
)
PARTITIONED BY (date_debut STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table hotels partitionnée
CREATE TABLE hotels_partitioned (
  hotel_id INT,
  nom STRING,
  etoiles INT
)
PARTITIONED BY (ville STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table reservations bucketée
CREATE TABLE reservations_bucketed (
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