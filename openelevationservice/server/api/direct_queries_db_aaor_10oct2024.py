from typing import List, Tuple
from openelevationservice.server.grpc.db_grpc import db
from sqlalchemy import text
from shapely.wkt import loads


# ELEVATION_UNION_FORMAT = text(
#     """
# WITH query_geom AS (
# 	SELECT ST_SetSRID(ST_MakePolygon(
# 		ST_GeomFromText(:polygon)
# 	), 4326) AS geom
# )
# SELECT jsonb_build_object(
# 	'type', 'FeatureCollection',
# 	'features', jsonb_agg(jsonb_build_object(
# 		'type', 'Feature',
# 		'geometry', ST_AsGeojson(geometry),
# 		'properties', json_build_object(
# 			'height', height
# 		)
# 	))
# ), MIN(height), MAX(height), AVG(height)
# FROM (
# 	SELECT val AS height, ST_SimplifyPreserveTopology(ST_Union(array_agg(ST_ReducePrecision(geom, 1e-12))), 1e-12) AS geometry
# 	FROM (
# 		SELECT DISTINCT (ST_PixelAsPolygons(
# 			ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
# 			1, False
# 		)).*
# 		FROM query_geom JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
# 	) AS tr
# 	GROUP BY val
# ) AS features
# """
# )
#print('gooooooooooooooooooooooooooo',ELEVATION_UNION_FORMAT)

# def direct_conexion_query_db_aaor(
#     geometry: List[Tuple[float, float]]
# ) -> Tuple[dict, float, float, float]:
#     """Get elevation data for a polygon geometry, united by range of elevation values."""
#     coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geometry)
#     polygon = f"LINESTRING({coordinates})"
#     print("conexion vale")
#     #print(coordinates)

#     return (
#         db.get_session().execute(ELEVATION_UNION_FORMAT, {"polygon": polygon}).scalar()
#     )




def main(geom):

    #########(1)--> prueba de conexion directa

    # Obtener una sesión activa de SQLAlchemy
    # session = db.get_session()

    # # Consulta para obtener la versión de PostgreSQL
    # version_query = text("SELECT version();")
    # # Consulta para obtener la versión de PostGIS (si está instalado)
    # postgis_query = text("SELECT PostGIS_Version();")

    # try:
    #     # Ejecutar la consulta para obtener la versión de PostgreSQL
    #     postgres_version = session.execute(version_query).scalar()
    #     print(f"Versión de PostgreSQL: {postgres_version}")
        
    #     # Ejecutar la consulta para obtener la versión de PostGIS
    #     postgis_version = session.execute(postgis_query).scalar()
    #     print(f"Versión de PostGIS: {postgis_version}")

    # except Exception as e:
    #     print(f"Error al conectar o ejecutar la consulta: {e}")

    # finally:
    #     # Cerrar la sesión para liberar recursos
    #     session.close()
    
    ###########(2)-->prueba si esta activa la paralelización

    # # Obtener una sesión activa de SQLAlchemy
    # session = db.get_session()

    # # Consulta para obtener los parámetros relacionados con la paralelización
    # parallel_query = text("""
    #     SELECT 
    #         name, 
    #         setting
    #     FROM pg_settings
    #     WHERE name IN (
    #         'max_parallel_workers', 
    #         'max_parallel_workers_per_gather', 
    #         'parallel_tuple_cost', 
    #         'parallel_setup_cost',
    #         'force_parallel_mode'
    #     );
    # """)

    # try:
    #     # Ejecutar la consulta
    #     parallel_settings = session.execute(parallel_query).fetchall()

    #     # Imprimir los valores de los parámetros relacionados con la paralelización
    #     for setting in parallel_settings:
    #         # setting[0] es el nombre del parámetro, setting[1] es el valor
    #         print("Configuración de BBDD para paralelizar")
    #         print(f"{setting[0]}: {setting[1]}")

    # except Exception as e:
    #     print(f"Error al consultar los parámetros de paralelización: {e}")

    # finally:
    #     # Cerrar la sesión
    #     session.close()
    
    ###########(3)-->prueba para verificar si se usa paralelización en las consultas

    # Obtener una sesión activa de SQLAlchemy
    # session = db.get_session()

    # # Consulta para verificar si se usa paralelización
    # explain_query = text("""
    #     SELECT a.rid, COUNT(*)
    #     FROM public.oes_cgiar a
    #     JOIN cron.job_run_details b ON a.rid = b.job_id  -- Asegúrate de que job_id sea válido
    #     GROUP BY a.rid;
    # """)

    # try:
    #     # Ejecutar la consulta EXPLAIN ANALYZE
    #     explain_result = session.execute(explain_query).fetchall()

    #     # Imprimir el plan de ejecución
    #     for row in explain_result:
    #         print(row[0])

    # except Exception as e:
    #     print(f"Error al ejecutar EXPLAIN ANALYZE: {e}")

    # finally:
    #     # Cerrar la sesión
    #     session.close()

    ###########(4)-->verificar tablas existentes en la BBDD

    # # Obtener una sesión activa de SQLAlchemy
    # session = db.get_session()

    # # Consulta para listar las tablas disponibles
    # list_tables_query = text("""
    #     SELECT table_schema, table_name
    #     FROM information_schema.tables
    #     WHERE table_type = 'BASE TABLE' 
    #     AND table_schema NOT IN ('information_schema', 'pg_catalog');
    # """)

    # try:
    #     # Ejecutar la consulta para listar tablas
    #     tables = session.execute(list_tables_query).fetchall()

    #     # Imprimir las tablas disponibles
    #     for table in tables:
    #         print(f"Esquema: {table[0]}, Tabla: {table[1]}")

    # except Exception as e:
    #     print(f"Error al listar las tablas: {e}")

    # finally:
    #     # Cerrar la sesión
    #     session.close()

    ###########(5)-->verificar de columnas existentesen la BBDD

    # Obtener una sesión activa de SQLAlchemy
    # session = db.get_session()

    # # Consulta para obtener los nombres de las columnas de oes_cgiar
    # columns_query = text("""
    #     SELECT column_name
    #     FROM information_schema.columns
    #     WHERE table_name = 'oes_cgiar';
    # """)

    # try:
    #     # Ejecutar la consulta
    #     columns = session.execute(columns_query).fetchall()

    #     # Imprimir los nombres de las columnas
    #     for column in columns:
    #         print(column[0])

    # except Exception as e:
    #     print(f"Error al obtener las columnas: {e}")

    # finally:
    #     # Cerrar la sesión
    #     session.close()


    ###########(6)-->verificar de paralelización con una consulta costosa

    
    # Obtener una sesión activa de SQLAlchemy
    session = db.get_session()

    # Definir la consulta
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
        )),
        MIN(height), MAX(height), AVG(height)
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

    # Ejecutar la consulta EXPLAIN ANALYZE
    explain_query = text("EXPLAIN ANALYZE " + str(ELEVATION_UNION_FORMAT))

    try:

        #Obtener las coordenadas del polígono en formato lista
        print(" 1 ")
        geometry: List[Tuple[float, float]]
        geometry = list(geom.exterior.coords)
        print("geometry-",geometry)
        # Define un polígono en formato WKT
        coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom)
        print("coordinates",coordinates)
        polygon = f"LINESTRING({coordinates})"
        print(" ")
        print("polygon vale",polygon)
        #polygon = 'POLYGON((...))'  # Reemplaza con el valor correcto de tu polígono

        print("")
        print("geometry_list")
        print(geometry)
        print("")
        # Ejecutar la consulta EXPLAIN ANALYZE con el parámetro del polígono
        explain_result = session.execute(explain_query, {"polygon": polygon}).fetchall()

        # Imprimir el plan de ejecución
        for row in explain_result:
            print(row[0])

    except Exception as e:
        print(f"Error al ejecutar EXPLAIN ANALYZE: {e}")

    finally:
        # Cerrar la sesión
        session.close()


    ###########(llegue al final)

        
    
    return direct_conexion_query_db_aaor(geometry)
