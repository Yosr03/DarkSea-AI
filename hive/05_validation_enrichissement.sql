USE darksea;

SET hive.execution.engine=mr;
SET mapreduce.framework.name=local;

-- ==========================================
-- TEST 1 : NOMBRE DE LIGNES
-- ==========================================

SELECT 'zones_maritimes' AS table_name,
       COUNT(*) AS nb_lignes
FROM zones_maritimes

UNION ALL

SELECT 'statistiques_mensuelles', COUNT(*)
FROM statistiques_mensuelles

UNION ALL

SELECT 'statistiques_pavillons', COUNT(*)
FROM statistiques_pavillons

UNION ALL

SELECT 'statistiques_types_navires', COUNT(*)
FROM statistiques_types_navires

UNION ALL

SELECT 'statistiques_distance_cote', COUNT(*)
FROM statistiques_distance_cote

UNION ALL

SELECT 'evenements_cartographie', COUNT(*)
FROM evenements_cartographie;

-- ==========================================
-- TEST 2 : CONSERVATION DES EVENEMENTS
-- ==========================================

SELECT 'zones_maritimes' AS source,
       SUM(nb_interruptions) AS total_evenements
FROM zones_maritimes

UNION ALL

SELECT 'statistiques_mensuelles',
       SUM(nb_interruptions)
FROM statistiques_mensuelles

UNION ALL

SELECT 'statistiques_pavillons',
       SUM(nb_interruptions)
FROM statistiques_pavillons

UNION ALL

SELECT 'statistiques_types_navires',
       SUM(nb_interruptions)
FROM statistiques_types_navires

UNION ALL

SELECT 'statistiques_distance_cote',
       SUM(nb_interruptions)
FROM statistiques_distance_cote

UNION ALL

SELECT 'evenements_cartographie', COUNT(*)
FROM evenements_cartographie;

-- ==========================================
-- TEST 3 : QUALITE DES DONNEES
-- ==========================================

SELECT
    COUNT(*) AS total_evenements,
    COUNT(DISTINCT gap_id) AS gap_id_uniques,
    COUNT(DISTINCT mmsi) AS nb_navires,
    SUM(CASE WHEN pavillon = 'UNKNOWN'
        THEN 1 ELSE 0 END) AS pavillons_manquants,
    SUM(duree_extreme) AS durees_extremes,
    SUM(mmsi_format_atypique) AS mmsi_atypiques,
    SUM(CASE WHEN gap_hours IS NULL
        THEN 1 ELSE 0 END) AS durees_nulles
FROM evenements_cartographie;

-- ==========================================
-- TEST 4 : COORDONNEES DE FIN
-- ==========================================

SELECT COUNT(*) AS coordonnees_fin_invalides
FROM evenements_cartographie
WHERE latitude_fin IS NULL
   OR longitude_fin IS NULL
   OR latitude_fin < -90
   OR latitude_fin > 90
   OR longitude_fin < -180
   OR longitude_fin > 180;

-- ==========================================
-- TEST 5 : TOP 10 ZONES MARITIMES
-- ==========================================

SELECT
    cellule_lat,
    cellule_lon,
    nb_interruptions,
    nb_navires,
    ROUND(duree_moyenne_h, 2) AS duree_moyenne_h
FROM zones_maritimes
ORDER BY nb_interruptions DESC
LIMIT 10;

-- ==========================================
-- TEST 6 : EVOLUTION ANNUELLE
-- ==========================================

SELECT
    SUBSTR(mois, 1, 4) AS annee,
    SUM(nb_interruptions) AS nb_interruptions,
    ROUND(SUM(duree_totale_h), 2) AS total_heures,
    ROUND(
        SUM(duree_totale_h) / SUM(nb_interruptions),
        2
    ) AS duree_moyenne_h
FROM statistiques_mensuelles
GROUP BY SUBSTR(mois, 1, 4)
ORDER BY annee;

-- ==========================================
-- TEST 7 : REPARTITION PAR DISTANCE
-- ==========================================

SELECT
    zone_distance,
    nb_interruptions,
    nb_navires,
    ROUND(nb_interruptions * 100.0 / 55368, 2)
        AS pourcentage_dataset,
    ROUND(duree_moyenne_h, 2) AS duree_moyenne_h
FROM statistiques_distance_cote
ORDER BY nb_interruptions DESC;

-- ==========================================
-- TEST 8 : JOINTURE EVENEMENTS ET IA
-- ==========================================

SELECT
    e.gap_id,
    e.mmsi,
    e.pavillon,
    e.vessel_class,
    e.latitude,
    e.longitude,
    ROUND(e.gap_hours, 2) AS gap_hours,
    p.cluster,
    ROUND(p.distance_centroide, 4) AS score_anomalie,
    p.anomalie_potentielle
FROM evenements_cartographie e
JOIN profils_scores p
    ON e.mmsi = p.mmsi
WHERE p.anomalie_potentielle = TRUE
ORDER BY p.distance_centroide DESC
LIMIT 20;

-- ==========================================
-- TEST 9 : COUVERTURE DE LA JOINTURE ML
-- ==========================================

SELECT
    COUNT(*) AS total_lignes_jointure,
    COUNT(DISTINCT e.gap_id) AS evenements_uniques,
    SUM(CASE WHEN p.mmsi IS NULL
        THEN 1 ELSE 0 END) AS evenements_sans_profil_ml
FROM evenements_cartographie e
LEFT JOIN profils_scores p
    ON e.mmsi = p.mmsi;