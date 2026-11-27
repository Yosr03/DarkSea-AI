from pyspark.sql import functions as F

FEATURE_COLUMNS = [
    "gap_hours",
    "distance_cote_km",
    "deplacement_km",
    "vitesse_apparente_kmh"
]

def prepare_event_features(df):

    lat1 = F.radians(F.col("latitude"))
    lon1 = F.radians(F.col("longitude"))

    lat2 = F.radians(F.col("latitude_fin"))
    lon2 = F.radians(F.col("longitude_fin"))

    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1

    a = (
        F.pow(F.sin(delta_lat / 2), 2)
        + F.cos(lat1)
        * F.cos(lat2)
        * F.pow(F.sin(delta_lon / 2), 2)
    )

    a = F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))

    distance = (
        F.lit(6371.0088)
        * 2
        * F.asin(F.sqrt(a))
    )

    df = df.withColumn(
        "deplacement_km",
        distance
    )

    df = df.withColumn(
        "vitesse_apparente_kmh",
        F.when(
            F.col("gap_hours") > 0,
            F.col("deplacement_km") / F.col("gap_hours")
        )
    )

    return df


def validate_events(df):

    condition = (
        F.col("gap_id").isNotNull()
        & (F.length(F.trim(F.col("gap_id"))) > 0)
        & F.col("mmsi").isNotNull()
        & (F.length(F.trim(F.col("mmsi"))) > 0)
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & F.col("latitude_fin").between(-90, 90)
        & F.col("longitude_fin").between(-180, 180)
        & (F.col("gap_hours") > 0)
        & (F.col("distance_cote_km") >= 0)
    )

    for name in [
        "latitude",
        "longitude",
        "latitude_fin",
        "longitude_fin",
        "gap_hours",
        "distance_cote_km"
    ]:
        condition = (
            condition
            & F.col(name).isNotNull()
            & ~F.isnan(F.col(name))
            & (F.abs(F.col(name)) < float("inf"))
        )

    return df.withColumn(
        "event_valid",
        F.coalesce(condition, F.lit(False))
    )