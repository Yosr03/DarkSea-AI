
-- DarkSea AI : test de lecture du dataset AIS

ais = LOAD 'hdfs://namenode:9000/darksea/raw/ais_disabling_events.csv'
USING PigStorage(',')
AS (
    gap_id:chararray,
    mmsi:chararray,
    vessel_class:chararray,
    flag:chararray,
    vessel_length_m:double,
    vessel_tonnage_gt:double,
    gap_start_timestamp:chararray,
    gap_start_lat:double,
    gap_start_lon:double,
    gap_start_distance_from_shore_m:double,
    gap_end_timestamp:chararray,
    gap_end_lat:double,
    gap_end_lon:double,
    gap_end_distance_from_shore_m:double,
    gap_hours:double
);

-- Exclure l'en-tête CSV
donnees = FILTER ais BY gap_id != 'gap_id';

-- Afficher trois événements AIS
exemples = LIMIT donnees 3;

DUMP exemples;
