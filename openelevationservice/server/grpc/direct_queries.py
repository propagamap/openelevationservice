from typing import List, Tuple
from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text

TILES_PER_DEGREE = 1200


ELEVATION_AREA_FORMAT = text(
    """
  WITH query_geom AS (
	SELECT ST_SetSRID(ST_MakePolygon(
		ST_GeomFromText(:polygon)
	), 4326) AS geom
)
SELECT ST_X(tr.geom), ST_Y(tr.geom), val
FROM (
	SELECT DISTINCT (ST_PixelAsCentroids(
		ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
		1, False
	)).*, rid
	FROM query_geom JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
	WHERE rid < 10000000
) AS tr JOIN query_geom ON ST_Within(tr.geom, query_geom.geom)
ORDER BY ST_Y(tr.geom), ST_X(tr.geom)
"""
)


def area_elevation(
    geometry: List[Tuple[float, float]]
) -> List[Tuple[float, float, float]]:
    """Get elevation data for an area geometry."""
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
    polygon = f"LINESTRING({coordinates})"

    return db.get_session().execute(ELEVATION_AREA_FORMAT, {"polygon": polygon}).all()


ELEVATION_UNION_FORMAT = text(
    """
WITH query_geom AS (
	SELECT ST_SetSRID(ST_MakePolygon(
		ST_GeomFromText(:polygon)
	), 4326) AS geom
)
SELECT jsonb_build_object(
	'type', 'FeatureCollection',
	'features', jsonb_agg(jsonb_build_object(
		'type', 'Feature',
		'geometry', ST_AsGeojson(geometry),
		'properties', json_build_object(
			'height', height
		)
	))
), MIN(height), MAX(height), AVG(height)
FROM (
	SELECT val AS height, ST_SimplifyPreserveTopology(ST_Union(array_agg(ST_ReducePrecision(geom, 1e-12))), 1e-12) AS geometry
	FROM (
		SELECT DISTINCT (ST_PixelAsPolygons(
			ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
			1, False
		)).*
		FROM query_geom JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
	) AS tr
	GROUP BY val
) AS features
"""
)


def polygon_coloring_elevation(
    geometry: List[Tuple[float, float]]
) -> Tuple[dict, float, float, float]:
    """Get elevation data for a polygon geometry, united by range of elevation values."""
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
    polygon = f"LINESTRING({coordinates})"

    return (
        db.get_session().execute(ELEVATION_UNION_FORMAT, {"polygon": polygon}).scalar()
    )


STRETCHED_AREA_FORMAT = text(
    """
WITH query_geom AS (
    SELECT ST_SetSRID(ST_MakePolygon(
        ST_GeomFromText(:polygon)
    ), 4326) AS geom
),
tiles AS (
    SELECT ST_PointN(ST_ExteriorRing(tr.geom), 1) AS top_left, val
    FROM (
        SELECT DISTINCT (ST_PixelAsPolygons(
            oes_cgiar.rast,
            1, False
        )).*
        FROM query_geom INNER JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
    ) AS tr JOIN query_geom ON ST_Intersects(tr.geom, query_geom.geom)
)
SELECT ST_X(top_left), ST_Y(top_left), val
FROM tiles
"""
)


def format_stretch_area(
    botLeft: Tuple[float, float],
    topRight: Tuple[float, float],
    stretchPoint: Tuple[float, float],
) -> List[Tuple[float, float]]:
    [lft, bot] = botLeft
    [rgt, top] = topRight
    [lon, lat] = stretchPoint

    if lon < lft:
        if lat < bot:
            # Replace bot-left
            shape = [(lon, lat), (rgt, bot), (rgt, top), (lft, top)]
        elif lat > top:
            # Replace top-left
            shape = [(lft, bot), (rgt, bot), (rgt, top), (lon, lat)]
        else:
            # Insert as left-middle
            shape = [(lft, bot), (rgt, bot), (rgt, top), (lft, top), (lon, lat)]
    elif lon > rgt:
        if lat < bot:
            # Replace bot-right
            shape = [(lft, bot), (lon, lat), (rgt, top), (lft, top)]
        elif lat > top:
            # Replace top-right
            shape = [(lft, bot), (rgt, bot), (lon, lat), (lft, top)]
        else:
            # Insert as right-middle
            shape = [(lft, bot), (rgt, bot), (lon, lat), (rgt, top), (lft, top)]
    else:
        if lat < bot:
            # Insert as bot-middle
            shape = [(lft, bot), (lon, lat), (rgt, bot), (rgt, top), (lft, top)]
        elif lat > top:
            # Insert as top-middle
            shape = [(lft, bot), (rgt, bot), (rgt, top), (lon, lat), (lft, top)]
        else:
            # No changes needed
            shape = [(lft, bot), (rgt, bot), (rgt, top), (lft, top)]

    shape.append(shape[0])  # Close the polygon
    return shape


def stretched_area_elevation(
    botLeft: Tuple[float, float],
    topRight: Tuple[float, float],
    stretchPoint: Tuple[float, float],
) -> List[Tuple[float, float, float]]:
    geometry = format_stretch_area(botLeft, topRight, stretchPoint)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
    polygon = f"LINESTRING({coordinates})"

    return db.get_session().execute(STRETCHED_AREA_FORMAT, {"polygon": polygon}).all()
