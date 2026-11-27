
-- ==========================================
-- DarkSea-AI
-- Construction de la couche analytique Hive
-- ==========================================

CREATE DATABASE IF NOT EXISTS darksea;

USE darksea;

-- Table analytique issue du traitement Pig

CREATE EXTERNAL TABLE IF NOT EXISTS profils_navires (
    mmsi STRING,
    nb_interruptions BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/darksea/analytics/profils_navires';

-- Verification des tables

SHOW TABLES;

-- Verification de la structure

DESCRIBE profils_navires;

-- Affichage des premiers profils

SELECT * FROM profils_navires LIMIT 10;

-- Verification du nombre de profils

SELECT COUNT(*) AS nombre_navires
FROM profils_navires;

-- Statistiques comportementales globales

SELECT
    COUNT(*) AS nombre_navires,
    SUM(nb_interruptions) AS total_interruptions,
    AVG(duree_moyenne_h) AS moyenne_par_navire_h,
    MAX(duree_max_h) AS interruption_maximale_h,
    SUM(duree_totale_h) AS duree_totale_h
FROM profils_navires;

-- Top 10 des navires selon la duree cumulee

SELECT
    mmsi,
    nb_interruptions,
    duree_moyenne_h,
    duree_max_h,
    duree_totale_h
FROM profils_navires
ORDER BY duree_totale_h DESC
LIMIT 10;
