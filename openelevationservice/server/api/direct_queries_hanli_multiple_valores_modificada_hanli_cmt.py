from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text
import time

# Consulta SQL optimizada
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
    SELECT val AS height, 
           ST_SimplifyPreserveTopology(
               ST_Union(array_agg(ST_ReducePrecision(geom, 1e-12))), 1e-12
           ) AS geometry
    FROM (
        SELECT DISTINCT (ST_PixelAsPolygons(
            ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
            1, False
        )).*
        FROM query_geom 
        JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
    ) AS tr
    WHERE val > 0  -- Filtrar las alturas mayores a cero
    GROUP BY val
) AS features
"""
)

def main(geom):
    """Procesa los datos de elevación para una geometría de polígono."""
    # Extraer las coordenadas del polígono
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    result = db.get_session().execute(ELEVATION_UNION_FORMAT, {"polygon": polygon}).fetchone()
    fin = time.perf_counter()

    # Desempaquetar resultados y tiempo de ejecución
    geojson, min_height, max_height, avg_height = result
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")

    # Imprimir y devolver resultados
    print(f"GeoJSON: {geojson}")
    print(f"Altura mínima: {min_height}, Altura máxima: {max_height}, Altura promedio: {avg_height}")

    return geojson, min_height, max_height, avg_height
