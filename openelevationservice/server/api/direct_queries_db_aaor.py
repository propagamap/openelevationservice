from typing import List, Tuple
from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text

def main(geom):

    # Obtener una sesión activa de SQLAlchemy
    print(" ")
    print("Entrando a la conasulta directa a BBDD")
    print(" ")
    session = db.get_session()

    # Eliminar ST_MakePolygon y usar el polígono directamente
    ELEVATION_UNION_FORMAT_EXPLAIN = text(
    """
    SET max_parallel_workers_per_gather = 4;  -- Forzamos la paralelización con 4 trabajadores
    
    EXPLAIN ANALYZE
    WITH query_geom AS (
        SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
    )
    SELECT jsonb_build_object(
        'type', 'FeatureCollection',
        'features', jsonb_agg(jsonb_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeojson(geometry),
            'properties', json_build_object(
                'heightBase', height
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
            FROM query_geom 
            JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
        ) AS tr
        GROUP BY val
    ) AS features
    """
    )

    try:
        # Utilizar el polígono que ya llega como parámetro geom
        polygon = geom.wkt  # Si geom ya es un objeto Shapely, podemos usar su WKT directamente

        # Ejecutar la consulta EXPLAIN ANALYZE con el polígono recibido
        explain_result = session.execute(ELEVATION_UNION_FORMAT_EXPLAIN, {"polygon": polygon}).fetchall()

        # Imprimir el plan de ejecución
        for row in explain_result:
            print(row[0])
        print("¡¡¡¡¡¡¡¡¡se ejecuto correctamente la consulta...!!!!!!!!")

    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return {}, (None, None), None

    finally:
        # Cerrar la sesión
        session.close()

    return explain_result

# def main(geom):
#     # Obtener una sesión activa de SQLAlchemy
#     session = db.get_session()

#     # Consulta ajustada para EXPLAIN ANALYZE
#     ELEVATION_UNION_FORMAT_EXPLAIN = text(
#     """
#     SET max_parallel_workers_per_gather = 4;  -- Forzamos la paralelización con 4 trabajadores
    
#     EXPLAIN ANALYZE
#     WITH query_geom AS (
#         SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
#     )
#     SELECT jsonb_build_object(
#         'type', 'FeatureCollection',
#         'features', jsonb_agg(jsonb_build_object(
#             'type', 'Feature',
#             'geometry', ST_AsGeojson(geometry),
#             'properties', json_build_object(
#                 'heightBase', height
#             )
#         ))
#     ) AS features_collection, 
#     MIN(height) AS min_height, 
#     MAX(height) AS max_height, 
#     AVG(height) AS avg_height
#     FROM (
#         SELECT val AS height, ST_SimplifyPreserveTopology(ST_Union(array_agg(ST_ReducePrecision(geom, 1e-12))), 1e-12) AS geometry
#         FROM (
#             SELECT DISTINCT (ST_PixelAsPolygons(
#                 ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
#                 1, False
#             )).*
#             FROM query_geom 
#             JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
#         ) AS tr
#         GROUP BY val
#     ) AS features
#     """
#     )

#     try:
#         # Utilizar el polígono que ya llega como parámetro geom
#         polygon = geom.wkt  # Si geom ya es un objeto Shapely, podemos usar su WKT directamente
#         print("poligono",polygon)

#         # Ejecutar la consulta con el polígono recibido
#         explain_result = session.execute(ELEVATION_UNION_FORMAT_EXPLAIN, {"polygon": polygon}).fetchall()

#         # Imprimir el resultado de la consulta para depuración
#         print("Plan de ejecución:")
#         for row in explain_result:
#             print(row[0])  # Imprimir cada fila del plan

#     except Exception as e:
#         print(f"Error al ejecutar la consulta: {e}")
#         return {}, (None, None), None

#     finally:
#         # Cerrar la sesión
#         session.close()

#     return explain_result  # Devolver el resultado del plan de ejecución
