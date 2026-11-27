
USE darksea;

SET hive.execution.engine=mr;
SET mapreduce.framework.name=local;

-- ==========================================
-- 1. Profils comportementaux enrichis
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS profils_enrichis (
    mmsi STRING,
    nb_interruptions BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE,
    ratio_duree_max DOUBLE,
    duree_totale_jours DOUBLE,
    classe_frequence STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/darksea/analytics/profils_enrichis';

INSERT OVERWRITE TABLE profils_enrichis
SELECT
    mmsi,
    nb_interruptions,
    duree_moyenne_h,
    duree_max_h,
    duree_totale_h,

    CASE
        WHEN duree_totale_h > 0
        THEN duree_max_h / duree_totale_h
        ELSE 0
    END,

    duree_totale_h / 24.0,

    CASE
        WHEN nb_interruptions >= 50 THEN 'TRES_ELEVEE'
        WHEN nb_interruptions >= 20 THEN 'ELEVEE'
        WHEN nb_interruptions >= 5 THEN 'MOYENNE'
        ELSE 'FAIBLE'
    END

FROM profils_navires;

-- ==========================================
-- 2. Statistiques globales
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS statistiques_globales (
    nombre_navires BIGINT,
    total_interruptions BIGINT,
    duree_moyenne_evenement_h DOUBLE,
    interruption_maximale_h DOUBLE,
    duree_totale_h DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/darksea/analytics/statistiques_globales';

INSERT OVERWRITE TABLE statistiques_globales
SELECT
    COUNT(*),
    SUM(nb_interruptions),
    SUM(duree_totale_h) / SUM(nb_interruptions),
    MAX(duree_max_h),
    SUM(duree_totale_h)
FROM profils_navires;

-- ==========================================
-- 3. Navires prioritaires pour analyse
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS navires_prioritaires (
    mmsi STRING,
    nb_interruptions BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/darksea/analytics/navires_prioritaires';

INSERT OVERWRITE TABLE navires_prioritaires
SELECT
    mmsi,
    nb_interruptions,
    duree_moyenne_h,
    duree_max_h,
    duree_totale_h
FROM profils_navires
WHERE nb_interruptions >= 50
   OR duree_max_h >= 168;

-- ==========================================
-- 4. Validation
-- ==========================================

SHOW TABLES;

SELECT COUNT(*) FROM profils_enrichis;

SELECT * FROM profils_enrichis LIMIT 10;

SELECT * FROM statistiques_globales;

SELECT COUNT(*) FROM navires_prioritaires;

SELECT
    classe_frequence,
    COUNT(*) AS nombre_navires
FROM profils_enrichis
GROUP BY classe_frequence;

SELECT * FROM navires_prioritaires
ORDER BY duree_totale_h DESC
LIMIT 10;
