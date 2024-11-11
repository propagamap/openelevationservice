from typing import List, Tuple
from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text
from shapely.wkt import loads
import time

#query Code original
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
#End-query Code original

#print('gooooooooooooooooooooooooooo',ELEVATION_UNION_FORMAT)

def polygon_coloring_elevation(
    geometry: List[Tuple[float, float]]
) -> Tuple[dict, float, float, float]:
    """Get elevation data for a polygon geometry, united by range of elevation values."""
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
    polygon = f"LINESTRING({coordinates})"

    result = db.get_session().execute(ELEVATION_UNION_FORMAT, {"polygon": polygon}).fetchone()
    

    return result[0], result[1], result[2], result[3]



def main(geom):
    geometry = list(geom.exterior.coords)

    # Recoge los resultados de la consulta
       
    inicio_consulta_queried_hanli = time.perf_counter()
    geojson, min_height, max_height, avg_height = polygon_coloring_elevation(geometry)
    fin_consulta_queried_hanli = time.perf_counter()
    
    tiempo_transcurrido_consulta_queried_hanli = fin_consulta_queried_hanli - inicio_consulta_queried_hanli
    print(f"Tiempo de ejecución_consulta_queried_hanli: {tiempo_transcurrido_consulta_queried_hanli:.6f} segundos")

    # print("OriginalcodeHanli")
    print("Original code Hanli")
    # print(geojson)
    print(f"Minimum height: {min_height}")
    print(f"Maximum height: {max_height}")
    print(f"Average height: {avg_height}")

    return geojson, min_height, max_height, avg_height


