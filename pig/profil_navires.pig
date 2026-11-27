
-- DarkSea AI
-- Construction des profils comportementaux des navires

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

-- Conserver les événements avec un navire identifié
-- et une durée exploitable
donnees = FILTER ais BY
    gap_id != 'gap_id'
    AND mmsi IS NOT NULL
    AND gap_hours IS NOT NULL
    AND gap_hours > 0;

-- Regrouper les interruptions par navire
groupes = GROUP donnees BY mmsi;

-- Produire de nouvelles connaissances
profils = FOREACH groupes GENERATE
    group AS mmsi,
    COUNT(donnees) AS nb_interruptions,
    AVG(donnees.gap_hours) AS duree_moyenne_h,
    MAX(donnees.gap_hours) AS duree_max_h,
    SUM(donnees.gap_hours) AS duree_totale_h;

-- Afficher cinq profils pour vérification
exemples = LIMIT profils 5;
DUMP exemples;


-- Enregistrer tous les profils dans HDFS
STORE profils
INTO 'hdfs://namenode:9000/darksea/analytics/profils_navires'
USING PigStorage(',');
