from typing import List, Tuple
from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text

TILES_PER_DEGREE = 1200


def format_area(
    bot_left: Tuple[float, float], top_right: Tuple[float, float]
) -> List[Tuple[float, float]]:
    [lft, bot] = bot_left
    [rgt, top] = top_right
    return [(lft, bot), (rgt, bot), (rgt, top), (lft, top), (lft, bot)]


ELEVATION_AREA_STATEMENT = text(
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

    return (
        db.get_session().execute(ELEVATION_AREA_STATEMENT, {"polygon": polygon}).all()
    )


ELEVATION_UNION_STATEMENT = text(
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
        db.get_session()
        .execute(ELEVATION_UNION_STATEMENT, {"polygon": polygon})
        .scalar()
    )


STRETCHED_AREA_STATEMENT = text(
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
    bot_left: Tuple[float, float],
    top_right: Tuple[float, float],
    stretch_point: Tuple[float, float],
) -> List[Tuple[float, float]]:
    [lft, bot] = bot_left
    [rgt, top] = top_right
    [lon, lat] = stretch_point

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
    bot_left: Tuple[float, float],
    top_right: Tuple[float, float],
    stretch_point: Tuple[float, float],
) -> List[Tuple[float, float, float]]:
    geometry = format_stretch_area(bot_left, top_right, stretch_point)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
    polygon = f"LINESTRING({coordinates})"

    return (
        db.get_session().execute(STRETCHED_AREA_STATEMENT, {"polygon": polygon}).all()
    )


EXTENDED_AREA_FORMAT = """
WITH query_geom AS (
    SELECT ST_SetSRID(ST_Union(
        ARRAY[{formatted_params}]
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


def extended_area_elevation(
    bot_left: Tuple[float, float],
    top_right: Tuple[float, float],
    extend_points: List[Tuple[float, float]],
) -> List[Tuple[float, float, float]]:

    if len(extend_points) == 0:
        geometry = format_area(bot_left, top_right)
        coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
        polygon = f"LINESTRING({coordinates})"
        return (
            db.get_session()
            .execute(STRETCHED_AREA_STATEMENT, {"polygon": polygon})
            .all()
        )

    formatted_params = ", ".join(
        f"ST_GeomFromText(:polygon{i})" for i in range(len(extend_points))
    )
    statement = text(EXTENDED_AREA_FORMAT.format(formatted_params=formatted_params))

    params = {}
    for i, point in enumerate(extend_points):
        geometry = format_stretch_area(bot_left, top_right, point)
        coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
        params[f"polygon{i}"] = f"POLYGON(({coordinates}))"

    return db.get_session().execute(statement, params).all()
