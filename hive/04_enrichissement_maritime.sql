USE darksea;

-- ==========================================
-- 1. ZONES MARITIMES
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS zones_maritimes (
    cellule_lat INT,
    cellule_lon INT,
    nb_interruptions BIGINT,
    nb_navires BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/darksea/analytics/zones_maritimes';

-- ==========================================
-- 2. STATISTIQUES MENSUELLES
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS statistiques_mensuelles (
    mois STRING,
    nb_interruptions BIGINT,
    nb_navires BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/darksea/analytics/statistiques_mensuelles';

-- ==========================================
-- 3. STATISTIQUES PAR PAVILLON
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS statistiques_pavillons (
    pavillon STRING,
    nb_interruptions BIGINT,
    nb_navires BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/darksea/analytics/statistiques_pavillons';

-- ==========================================
-- 4. STATISTIQUES PAR TYPE DE NAVIRE
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS statistiques_types_navires (
    vessel_class STRING,
    nb_interruptions BIGINT,
    nb_navires BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/darksea/analytics/statistiques_types_navires';

-- ==========================================
-- 5. STATISTIQUES PAR DISTANCE DES COTES
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS statistiques_distance_cote (
    zone_distance STRING,
    nb_interruptions BIGINT,
    nb_navires BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/darksea/analytics/statistiques_distance_cote';

-- ==========================================
-- 6. EVENEMENTS POUR LA CARTOGRAPHIE
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS evenements_cartographie (
    gap_id STRING,
    mmsi STRING,
    vessel_class STRING,
    pavillon STRING,
    gap_start_timestamp STRING,
    gap_end_timestamp STRING,
    mois STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    latitude_fin DOUBLE,
    longitude_fin DOUBLE,
    distance_cote_km DOUBLE,
    gap_hours DOUBLE,
    zone_distance STRING,
    cellule_lat INT,
    cellule_lon INT,
    duree_extreme INT,
    mmsi_format_atypique INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/darksea/analytics/evenements_cartographie';

SHOW TABLES;