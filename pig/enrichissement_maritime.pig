
-- ==========================================
-- DARKSEA-AI
-- Enrichissement geographique et temporel
-- ==========================================

raw = LOAD
    'hdfs://namenode:9000/darksea/raw/ais_disabling_events.csv'
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

-- ==========================================
-- 1. CONTROLE DES EVENEMENTS
-- ==========================================

donnees = FILTER raw BY
    gap_id != 'gap_id'
    AND gap_id IS NOT NULL
    AND mmsi IS NOT NULL
    AND gap_hours IS NOT NULL
    AND gap_hours > 0;

-- ==========================================
-- 2. ENRICHISSEMENT DES EVENEMENTS
-- ==========================================

enrichis = FOREACH donnees GENERATE
    gap_id,
    mmsi,
    vessel_class,

    (
        flag IS NULL OR flag == ''
        ? 'UNKNOWN'
        : flag
    ) AS pavillon,

    gap_start_timestamp,
    gap_end_timestamp,

    SUBSTRING(gap_start_timestamp, 0, 4)
        AS annee,

    SUBSTRING(gap_start_timestamp, 0, 7)
        AS mois,

    gap_start_lat AS latitude,
    gap_start_lon AS longitude,

    gap_end_lat AS latitude_fin,
    gap_end_lon AS longitude_fin,

    gap_start_distance_from_shore_m / 1000.0
        AS distance_cote_km,

    gap_hours,

    (
        gap_start_distance_from_shore_m IS NULL
        OR gap_start_distance_from_shore_m < 0
        ? 'UNKNOWN'
        : (
            gap_start_distance_from_shore_m < 20000
            ? 'COTIERE'
            : (
                gap_start_distance_from_shore_m < 100000
                ? 'INTERMEDIAIRE'
                : (
                    gap_start_distance_from_shore_m < 370400
                    ? 'OFFSHORE'
                    : 'TRES_ELOIGNEE'
                )
            )
        )
    ) AS zone_distance,

    (
        gap_hours > 8760 ? 1 : 0
    ) AS duree_extreme,

    (
        mmsi MATCHES '^[0-9]{9}$'
        ? 0 : 1
    ) AS mmsi_format_atypique;

-- ==========================================
-- 3. VALIDATION GEOGRAPHIQUE
-- ==========================================

geo_valides = FILTER enrichis BY
    latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND latitude >= -90.0
    AND latitude <= 90.0
    AND longitude >= -180.0
    AND longitude <= 180.0;

geo_invalides = FILTER enrichis BY
    latitude IS NULL
    OR longitude IS NULL
    OR latitude < -90.0
    OR latitude > 90.0
    OR longitude < -180.0
    OR longitude > 180.0;

-- ==========================================
-- 4. CREATION DES CELLULES GEOGRAPHIQUES
-- ==========================================

cellules = FOREACH geo_valides GENERATE
    *,
    (
        latitude == 90.0
        ? 85
        : (int)(FLOOR(latitude / 5.0) * 5.0)
    ) AS cellule_lat,

    (
        longitude == 180.0
        ? 175
        : (int)(FLOOR(longitude / 5.0) * 5.0)
    ) AS cellule_lon;

group_geo = GROUP cellules BY
    (cellule_lat, cellule_lon);

zones_maritimes = FOREACH group_geo {

    navires = DISTINCT cellules.mmsi;

    GENERATE
        group.cellule_lat AS cellule_lat,
        group.cellule_lon AS cellule_lon,

        COUNT(cellules) AS nb_interruptions,
        COUNT(navires) AS nb_navires,

        AVG(cellules.gap_hours)
            AS duree_moyenne_h,

        MAX(cellules.gap_hours)
            AS duree_max_h,

        SUM(cellules.gap_hours)
            AS duree_totale_h;
};

-- ==========================================
-- 5. STATISTIQUES MENSUELLES
-- ==========================================

group_mois = GROUP enrichis BY mois;

statistiques_mensuelles = FOREACH group_mois {

    navires = DISTINCT enrichis.mmsi;

    GENERATE
        group AS mois,
        COUNT(enrichis) AS nb_interruptions,
        COUNT(navires) AS nb_navires,
        AVG(enrichis.gap_hours) AS duree_moyenne_h,
        MAX(enrichis.gap_hours) AS duree_max_h,
        SUM(enrichis.gap_hours) AS duree_totale_h;
};

-- ==========================================
-- 6. STATISTIQUES PAR PAVILLON
-- ==========================================

group_pavillons = GROUP enrichis BY pavillon;

statistiques_pavillons = FOREACH group_pavillons {

    navires = DISTINCT enrichis.mmsi;

    GENERATE
        group AS pavillon,
        COUNT(enrichis) AS nb_interruptions,
        COUNT(navires) AS nb_navires,
        AVG(enrichis.gap_hours) AS duree_moyenne_h,
        MAX(enrichis.gap_hours) AS duree_max_h,
        SUM(enrichis.gap_hours) AS duree_totale_h;
};

-- ==========================================
-- 7. STATISTIQUES PAR TYPE DE NAVIRE
-- ==========================================

group_types = GROUP enrichis BY vessel_class;

statistiques_types_navires = FOREACH group_types {

    navires = DISTINCT enrichis.mmsi;

    GENERATE
        group AS vessel_class,
        COUNT(enrichis) AS nb_interruptions,
        COUNT(navires) AS nb_navires,
        AVG(enrichis.gap_hours) AS duree_moyenne_h,
        MAX(enrichis.gap_hours) AS duree_max_h,
        SUM(enrichis.gap_hours) AS duree_totale_h;
};

-- ==========================================
-- 8. STATISTIQUES PAR DISTANCE DES COTES
-- ==========================================

group_distance = GROUP enrichis BY zone_distance;

statistiques_distance_cote = FOREACH group_distance {

    navires = DISTINCT enrichis.mmsi;

    GENERATE
        group AS zone_distance,
        COUNT(enrichis) AS nb_interruptions,
        COUNT(navires) AS nb_navires,
        AVG(enrichis.gap_hours) AS duree_moyenne_h,
        MAX(enrichis.gap_hours) AS duree_max_h,
        SUM(enrichis.gap_hours) AS duree_totale_h;
};

-- ==========================================
-- 9. DONNEES POUR LA CARTOGRAPHIE
-- ==========================================

evenements_cartographie = FOREACH cellules GENERATE
    gap_id,
    mmsi,
    vessel_class,
    pavillon,
    gap_start_timestamp,
    gap_end_timestamp,
    mois,
    latitude,
    longitude,
    latitude_fin,
    longitude_fin,
    distance_cote_km,
    gap_hours,
    zone_distance,
    cellule_lat,
    cellule_lon,
    duree_extreme,
    mmsi_format_atypique;

-- ==========================================
-- 10. CONTROLES DE QUALITE
-- ==========================================

g_total = GROUP enrichis ALL;

controle_total = FOREACH g_total GENERATE
    COUNT(enrichis) AS total_evenements,
    SUM(enrichis.gap_hours) AS total_heures,
    SUM(enrichis.duree_extreme) AS durees_extremes,
    SUM(enrichis.mmsi_format_atypique)
        AS evenements_mmsi_atypique;

g_geo = GROUP geo_valides ALL;

controle_geo = FOREACH g_geo GENERATE
    COUNT(geo_valides) AS coordonnees_valides;

DUMP controle_total;
DUMP controle_geo;

-- ==========================================
-- 11. STOCKAGE DES NOUVELLES CONNAISSANCES
-- ==========================================

STORE zones_maritimes INTO
    'hdfs://namenode:9000/darksea/analytics/zones_maritimes'
    USING PigStorage(',');

STORE statistiques_mensuelles INTO
    'hdfs://namenode:9000/darksea/analytics/statistiques_mensuelles'
    USING PigStorage(',');

STORE statistiques_pavillons INTO
    'hdfs://namenode:9000/darksea/analytics/statistiques_pavillons'
    USING PigStorage(',');

STORE statistiques_types_navires INTO
    'hdfs://namenode:9000/darksea/analytics/statistiques_types_navires'
    USING PigStorage(',');

STORE statistiques_distance_cote INTO
    'hdfs://namenode:9000/darksea/analytics/statistiques_distance_cote'
    USING PigStorage(',');

STORE evenements_cartographie INTO
    'hdfs://namenode:9000/darksea/analytics/evenements_cartographie'
    USING PigStorage(',');

STORE geo_invalides INTO
    'hdfs://namenode:9000/darksea/analytics/geo_invalides'
    USING PigStorage(',');
