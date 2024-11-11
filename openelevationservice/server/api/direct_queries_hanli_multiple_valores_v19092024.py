from typing import List, Tuple
from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text
from shapely.wkt import loads

#query Code original
# ELEVATION_UNION_FORMAT = text(
#     """
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
# """
# )
#End-query Code original

#New query code con refactoring
# ELEVATION_UNION_FORMAT = text(
#     """
# WITH query_geom AS (
#     SELECT ST_SetSRID(ST_MakePolygon(
#         ST_GeomFromText(:polygon)
#     ), 4326) AS geom
# )
# SELECT jsonb_build_object(
#     'type', 'FeatureCollection',
#     'features', jsonb_agg(jsonb_build_object(
#         'type', 'Feature',
#         'geometry', ST_AsGeojson(geometry),
#         'properties', json_build_object(
#             'height', height
#         )
#     ))
# ), MIN(height) AS min_height, MAX(height) AS max_height, AVG(height) AS avg_height
# FROM (
#     SELECT val AS height, 
#            ST_SimplifyPreserveTopology(ST_Union(array_agg(ST_ReducePrecision(geom, 1e-12))), 1e-12) AS geometry
#     FROM (
#         -- Filtramos alturas válidas (diferentes de NULL y de cero)
#         SELECT DISTINCT (ST_PixelAsPolygons(
#             ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
#             1, False
#         )).* 
#         FROM query_geom 
#         JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
#         WHERE ST_Value(oes_cgiar.rast, 1) IS NOT NULL AND ST_Value(oes_cgiar.rast, 1) <> 0
#     ) AS tr
#     GROUP BY val
# ) AS features
# """
# )
#End-New query code con refactoring

###
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
), MIN(height) AS min_height, MAX(height) AS max_height, AVG(height) AS avg_height
FROM (
    SELECT DISTINCT height, 
           ST_SimplifyPreserveTopology(ST_Union(geom), 1e-12) AS geometry
    FROM (
        SELECT (ST_PixelAsPolygons(
            ST_Clip(oes_cgiar.rast, query_geom.geom, 0), 1, False
        )).geom AS geom,
        (ST_PixelAsPolygons(
            ST_Clip(oes_cgiar.rast, query_geom.geom, 0), 1, False
        )).val AS height
        FROM query_geom 
        JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
        WHERE (ST_PixelAsPolygons(
            ST_Clip(oes_cgiar.rast, query_geom.geom, 0), 1, False
        )).val IS NOT NULL AND (ST_PixelAsPolygons(
            ST_Clip(oes_cgiar.rast, query_geom.geom, 0), 1, False
        )).val <> 0
    ) AS tr
    GROUP BY height
) AS features
"""
)

###

#print('gooooooooooooooooooooooooooo',ELEVATION_UNION_FORMAT)

def polygon_coloring_elevation(
    geometry: List[Tuple[float, float]]
) -> Tuple[dict, float, float, float]:
    """Get elevation data for a polygon geometry, united by range of elevation values."""
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
    polygon = f"LINESTRING({coordinates})"
    #print("coordinates")
    #print(coordinates)

    result = db.get_session().execute(ELEVATION_UNION_FORMAT, {"polygon": polygon}).fetchone()

    print("Se ejecuta la consulta",result)
    

    return result[0], result[1], result[2], result[3]


def main(geom):
    #Obtener las coordenadas del polígono en formato lista
    geometry = list(geom.exterior.coords)

    # Recoge los resultados de la consulta
    geojson, min_height, max_height, avg_height = polygon_coloring_elevation(geometry)

    # Imprime o procesa los resultados como desees
    print("GeoJSON FeatureCollection: entre aiiiiiiiiiiiiiiiiiiiiiiiiiiii:")
    # print(geojson)
    # print(f"Minimum height: {min_height}")
    # print(f"Maximum height: {max_height}")
    # print(f"Average height: {avg_height}")

    return geojson, min_height, max_height, avg_height

