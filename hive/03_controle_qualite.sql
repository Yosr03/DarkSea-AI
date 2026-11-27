
USE darksea;

SET hive.execution.engine=mr;
SET mapreduce.framework.name=local;

-- 1. Vérification des doublons
SELECT
    COUNT(*) AS total_profils,
    COUNT(DISTINCT mmsi) AS identifiants_uniques
FROM profils_enrichis;

-- 2. Vérification du format MMSI
SELECT
    LENGTH(mmsi) AS longueur_mmsi,
    COUNT(*) AS nombre_profils
FROM profils_enrichis
GROUP BY LENGTH(mmsi)
ORDER BY longueur_mmsi;

-- 3. Vérification des valeurs manquantes
SELECT COUNT(*) AS profils_incomplets
FROM profils_enrichis
WHERE mmsi IS NULL
   OR nb_interruptions IS NULL
   OR duree_moyenne_h IS NULL
   OR duree_max_h IS NULL
   OR duree_totale_h IS NULL
   OR ratio_duree_max IS NULL;

-- 4. Vérification de la cohérence
SELECT COUNT(*) AS profils_incoherents
FROM profils_enrichis
WHERE nb_interruptions <= 0
   OR duree_moyenne_h <= 0
   OR duree_max_h < duree_moyenne_h
   OR duree_totale_h < duree_max_h
   OR ratio_duree_max < 0
   OR ratio_duree_max > 1
   OR ABS(duree_totale_h -
          duree_moyenne_h * nb_interruptions) > 0.001;

-- 5. Distribution des durées maximales
SELECT
    MIN(duree_max_h) AS minimum_h,
    percentile_approx(duree_max_h, 0.5) AS mediane_h,
    percentile_approx(duree_max_h, 0.95) AS percentile_95,
    percentile_approx(duree_max_h, 0.99) AS percentile_99,
    MAX(duree_max_h) AS maximum_h
FROM profils_enrichis;

-- 6. Vérification des durées extrêmes
SELECT COUNT(*) AS profils_duree_superieure_un_an
FROM profils_enrichis
WHERE duree_max_h > 8760;
