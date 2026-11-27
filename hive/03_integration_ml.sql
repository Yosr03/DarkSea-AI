
USE darksea;

SET hive.execution.engine=mr;
SET mapreduce.framework.name=local;

-- ==========================================
-- 1. TABLE DES RESULTATS SPARK MLLIB
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS profils_scores (
    mmsi STRING,
    nb_interruptions BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE,
    classe_frequence STRING,
    mmsi_format_atypique BOOLEAN,
    duree_extreme BOOLEAN,
    cluster INT,
    distance_centroide DOUBLE,
    anomalie_potentielle BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/darksea/results/profils_scores';

-- ==========================================
-- 2. TABLE DES ANOMALIES ML
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS anomalies_ml (
    mmsi STRING,
    nb_interruptions BIGINT,
    duree_moyenne_h DOUBLE,
    duree_max_h DOUBLE,
    duree_totale_h DOUBLE,
    classe_frequence STRING,
    mmsi_format_atypique BOOLEAN,
    duree_extreme BOOLEAN,
    cluster INT,
    distance_centroide DOUBLE,
    anomalie_potentielle BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/darksea/analytics/anomalies_ml';

INSERT OVERWRITE TABLE anomalies_ml
SELECT *
FROM profils_scores
WHERE anomalie_potentielle = TRUE;

-- ==========================================
-- 3. SYNTHESE DES CLUSTERS
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS synthese_clusters (
    cluster INT,
    nombre_navires BIGINT,
    total_interruptions BIGINT,
    moyenne_interruptions DOUBLE,
    moyenne_duree_h DOUBLE,
    moyenne_duree_max_h DOUBLE,
    duree_totale_h DOUBLE,
    score_moyen DOUBLE,
    score_max DOUBLE,
    nombre_anomalies BIGINT,
    taux_anomalies DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/darksea/analytics/synthese_clusters';

INSERT OVERWRITE TABLE synthese_clusters
SELECT
    cluster,
    COUNT(*),
    SUM(nb_interruptions),
    AVG(nb_interruptions),
    AVG(duree_moyenne_h),
    AVG(duree_max_h),
    SUM(duree_totale_h),
    AVG(distance_centroide),
    MAX(distance_centroide),
    SUM(
        CASE
            WHEN anomalie_potentielle = TRUE
            THEN 1 ELSE 0
        END
    ),
    100.0 * SUM(
        CASE
            WHEN anomalie_potentielle = TRUE
            THEN 1 ELSE 0
        END
    ) / COUNT(*)
FROM profils_scores
GROUP BY cluster;

-- ==========================================
-- 4. COMPARAISON REGLES HIVE / ML
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS comparaison_regles_ml (
    categorie STRING,
    nombre_navires BIGINT,
    score_moyen DOUBLE,
    moyenne_interruptions DOUBLE,
    moyenne_duree_max_h DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/darksea/analytics/comparaison_regles_ml';

INSERT OVERWRITE TABLE comparaison_regles_ml
SELECT
    categorie,
    COUNT(*),
    AVG(distance_centroide),
    AVG(nb_interruptions),
    AVG(duree_max_h)
FROM (
    SELECT
        s.mmsi,
        s.distance_centroide,
        s.nb_interruptions,
        s.duree_max_h,
        CASE
            WHEN p.mmsi IS NOT NULL
                 AND s.anomalie_potentielle = TRUE
                THEN 'HIVE_ET_ML'

            WHEN p.mmsi IS NOT NULL
                 AND s.anomalie_potentielle = FALSE
                THEN 'HIVE_UNIQUEMENT'

            WHEN p.mmsi IS NULL
                 AND s.anomalie_potentielle = TRUE
                THEN 'ML_UNIQUEMENT'

            ELSE 'NON_SIGNALE'
        END AS categorie
    FROM profils_scores s
    LEFT JOIN (
        SELECT DISTINCT mmsi
        FROM navires_prioritaires
    ) p
    ON s.mmsi = p.mmsi
) comparaison
GROUP BY categorie;

-- ==========================================
-- 5. TESTS DE VALIDATION
-- ==========================================

SHOW TABLES;

SELECT COUNT(*) AS total_profils_scores
FROM profils_scores;

SELECT COUNT(*) AS total_anomalies
FROM anomalies_ml;

SELECT *
FROM synthese_clusters
ORDER BY cluster;

SELECT *
FROM comparaison_regles_ml
ORDER BY nombre_navires DESC;

SELECT SUM(nombre_navires) AS total_comparaison
FROM comparaison_regles_ml;

-- ==========================================
-- 6. TOP 10 DES PROFILS ATYPIQUES
-- ==========================================

SELECT
    mmsi,
    nb_interruptions,
    ROUND(duree_max_h, 2) AS duree_max_h,
    cluster,
    ROUND(distance_centroide, 4) AS score
FROM anomalies_ml
ORDER BY distance_centroide DESC
LIMIT 10;

-- ==========================================
-- 7. PROFILS DETECTES UNIQUEMENT PAR L'IA
-- ==========================================

SELECT
    s.mmsi,
    s.nb_interruptions,
    ROUND(s.duree_moyenne_h, 2) AS duree_moyenne_h,
    ROUND(s.duree_max_h, 2) AS duree_max_h,
    ROUND(s.distance_centroide, 4) AS score
FROM anomalies_ml s
LEFT JOIN navires_prioritaires p
ON s.mmsi = p.mmsi
WHERE p.mmsi IS NULL
ORDER BY s.distance_centroide DESC
LIMIT 10;

-- ==========================================
-- 8. QUALITE DES DONNEES ET ANOMALIES
-- ==========================================

SELECT
    mmsi_format_atypique,
    duree_extreme,
    anomalie_potentielle,
    COUNT(*) AS nombre_navires
FROM profils_scores
GROUP BY
    mmsi_format_atypique,
    duree_extreme,
    anomalie_potentielle;

-- ==========================================
-- 9. VERIFICATION DE LA COHERENCE
-- ==========================================

SELECT
    COUNT(*) AS total_profils,
    COUNT(DISTINCT mmsi) AS mmsi_uniques,
    SUM(
        CASE
            WHEN cluster IS NULL
              OR anomalie_potentielle IS NULL
              OR distance_centroide IS NULL
            THEN 1 ELSE 0
        END
    ) AS resultats_incomplets
FROM profils_scores;
