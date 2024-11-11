from typing import List, Tuple
from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text
from shapely.wkt import loads


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
#print('gooooooooooooooooooooooooooo',ELEVATION_UNION_FORMAT)

def polygon_coloring_elevation(
    geometry: List[Tuple[float, float]]
) -> Tuple[dict, float, float, float]:
    """Get elevation data for a polygon geometry, united by range of elevation values."""
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
    polygon = f"LINESTRING({coordinates})"
    #print("coordinates")
    #print(coordinates)

    return (
        db.get_session().execute(ELEVATION_UNION_FORMAT, {"polygon": polygon}).scalar()
    )


def main(geom):
    #Petición 1
    # geometry = [
    #     (-8.727183, 41.276104),
    #     (-8.731389, 41.276104),
    #     (-8.731389, 41.276636),
    #     (-8.727183, 41.276636),
    #     (-8.727183, 41.276104),
    # ]
    
	#Petición 2
    # geometry = [
    #     (-3.41314, 40.4762),
    #     (-3.28989, 40.4762), 
    #     (-3.28989, 40.91916),
    #     (-3.41314, 40.91916),
    #     (-3.41314, 40.4762),
    # ]
    
	#Petición 3-Area pequeña-ejemplo
    # print("geom este")
    # print(type(geom))
    # print(geom)
    # geometry = [
    #     (-7.32179, 43.18899),
    #     (-7.31953, 43.18899), 
    #     (-7.31953, 43.1906),
    #     (-7.32179, 43.1906),
    #     (-7.32179, 43.18899),
    # ]
    # print("geometry")
    # print(geometry)

    #Código para procesar area geom
    
    print("")
    print("geom 2")
    print(geom)

    #Obtener las coordenadas del polígono en formato lista
    geometry = list(geom.exterior.coords)
    print("")
    print("geometry_list")
    print(geometry)
    print("")

        
    
    return polygon_coloring_elevation(geometry)
