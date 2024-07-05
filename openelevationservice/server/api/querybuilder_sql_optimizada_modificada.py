# -*- coding: utf-8 -*-

from openelevationservice import SETTINGS
from openelevationservice.server.utils.logger import get_logger
#from openelevationservice.server.db_import.models import db, Cgiar
from openelevationservice.server.grpc.db_grpc import db, Cgiar
# from openelevationservice.server.utils.custom_func import ST_SnapToGrid
from openelevationservice.server.api.api_exceptions import InvalidUsage

from geoalchemy2.functions import ST_Value, ST_Intersects, ST_X, ST_Y # ST_DumpPoints, ST_Dump, 
from sqlalchemy import func, literal_column, case, text
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import array

#AAOR-i1
import time
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from shapely import wkb, geometry
import gc

def medir_tiempo(funcion):
    def funcion_medida(*args, **kwargs):
        inicio = time.perf_counter()
        resultado = funcion(*args, **kwargs)
        fin = time.perf_counter()
        tiempo_transcurrido = fin - inicio
        print(f"Tiempo de ejecución_1111111111111111111: {tiempo_transcurrido:.6f} segundos")
        return resultado

    return funcion_medida
#AAOR-f1

log = get_logger(__name__)

coord_precision = float(SETTINGS['coord_precision'])

NO_DATA_VALUE = -32768


#OJO
#AAOR __getModel modificada

def _getModel__(dataset):
    """
    Choose model based on dataset parameter and ensure spatial indexes exist.

    :param dataset: Elevation dataset to use for querying.
    :type dataset: str

    :returns: Corresponding database model.
    :rtype: SQLAlchemy model
    """
    model = None
    
    if dataset == 'srtm':
        model = Cgiar

    if model:
        session = db.get_session()  # Assuming db.get_session() provides a SQLAlchemy session
        inspector = inspect(session.get_bind())

        try:
            # Check for 'rast' index
            rast_index_exists = any(
                'rast' in index['column_names'] 
                for index in inspector.get_indexes(model.__tablename__)
            )

            if not rast_index_exists:
                print(f"Creating spatial index on '{model.__tablename__}.rast'...")
                session.connection().execute(text(f'''
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{model.__tablename__}_rast
                    ON {model.__tablename__}
                    USING GIST (ST_ConvexHull(rast));
                '''))

            # Check for 'geom' index in a generic way (assuming the model has a geometry column)
            geom_index_exists = any(
                'geom' in index['column_names']
                for index in inspector.get_indexes(model.__tablename__)
            )

            if not geom_index_exists:
                print(f"Creating spatial index on '{model.__tablename__}.geom'...")
                session.connection().execute(text(f'''
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{model.__tablename__}_geom
                    ON {model.__tablename__}
                    USING GIST (geom);
                '''))

        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
    
    return model

#OJO
#AAOR Fin __getModel modificada


#OJO
#AAOR __getModel Original
def _getModel(dataset):
    """
    Choose model based on dataset parameter
    
    :param dataset: elevation dataset to use for querying
    :type dataset: string
    
    :returns: database model
    :rtype: SQLAlchemy model
    """
    if dataset == 'srtm':
        model = Cgiar
    
    return model
#OJO
#AAOR Fin __getModel Original



def format_PixelAsGeoms(result_pixels):
    # format: [ ('(0101000020E61000000000000000C05240D169039D36003D40,202,1,2)',) , ...
    if (len(result_pixels) == 0): raise InvalidUsage(404, 4002, 'The requested geometry does not contain any elevation data')
    
    geoms = []
    heights = []
    for pixel in result_pixels:
        subcolumns = pixel[0].split(",")
        geoms.append(subcolumns[0][1: ])
        heights.append(int(subcolumns[1]))
    
    return func.unnest(literal_column("ARRAY{}".format(geoms))), \
           func.unnest(literal_column("ARRAY{}".format(heights)))


def polygon_coloring_elevation(geometry, dataset):
    """
    Performs PostGIS query to enrich a polygon geometry.
    
    :param geometry: Input 2D polygon to be enriched with elevation
    :type geometry: Shapely geometry
    
    :param dataset: Elevation dataset to use for querying
    :type dataset: string
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description. 
        
    :returns: 3D polygon as GeoJSON or WKT, range of elevation in the polygon
    :rtype: string
    """
    Model = _getModel(dataset)

    num_ranges = 23
    
    if geometry.geom_type == 'Polygon':
        query_geom = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326) \
                            .label('geom')) \
                            .subquery().alias('pGeom')

        result_pixels = db.get_session() \
                            .query(func.DISTINCT(func.ST_PixelAsPolygons(
                                func.ST_Clip(Model.rast, query_geom.c.geom, NO_DATA_VALUE),
                                1, False))) \
                            .select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))) \
                            .all()
        
        polygon_col, height_col = format_PixelAsGeoms(result_pixels)

        rebuilt_set = db.get_session() \
                            .query(polygon_col.label("geometry"),
                                   height_col.label("height")) \
                            .subquery().alias('rebuiltSet')

        filtered_set = db.get_session() \
                            .query(rebuilt_set.c.geometry,
                                   rebuilt_set.c.height) \
                            .select_from(rebuilt_set) \
                            .join(query_geom, func.ST_Within(func.ST_Centroid(rebuilt_set.c.geometry), query_geom.c.geom)) \
                            .subquery().alias('filteredSet')
        
        min_height, max_height, avg_height = db.get_session() \
                            .query(func.min(filtered_set.c.height),
                                   func.max(filtered_set.c.height),
                                   func.avg(filtered_set.c.height)) \
                            .select_from(filtered_set) \
                            .where(filtered_set.c.height != NO_DATA_VALUE) \
                            .one()
        
        if min_height is None or max_height is None:
            raise InvalidUsage(400, 4002,
                               'The requested geometry does not contain any elevation data')
        else:
            range_div = (max_height - min_height + 1) / num_ranges

            ranged_set = db.get_session() \
                                .query(filtered_set.c.geometry,
                                    func.GREATEST(
                                        func.LEAST(func.floor((filtered_set.c.height - min_height) / range_div), num_ranges), -1
                                    ).label("colorRange")) \
                                .select_from(filtered_set) \
                                .subquery().alias('rangedSet')

            query_features = db.get_session() \
                                .query(func.jsonb_build_object(
                                    'type', 'Feature',
                                    'geometry', func.ST_AsGeoJson(
                                        func.ST_SimplifyPreserveTopology(func.ST_Union(func.array_agg(
                                            func.ST_ReducePrecision(ranged_set.c.geometry, 1e-12)
                                        )), 1e-12)
                                    ).cast(JSON),
                                    'properties', func.json_build_object(
                                        'heightBase', case(
                                            (ranged_set.c.colorRange < 0, NO_DATA_VALUE),
                                            else_ = func.ceil(ranged_set.c.colorRange * range_div + min_height)
                                        ),
                                    )).label('features') \
                                ).select_from(ranged_set) \
                                .group_by(ranged_set.c.colorRange) \
                                .subquery().alias('rfeatures')

            # Return GeoJSON directly in PostGIS
            query_final = db.get_session() \
                                .query(func.jsonb_build_object(
                                    'type', 'FeatureCollection',
                                    'features', func.jsonb_agg(query_features.c.features))) \
                                .select_from(query_features)

    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))
    
    result_geom = query_final.scalar()

    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))

    return result_geom, [min_height, max_height], avg_height




#OJO 
#AAOR-->poligon_elevation optimizada-->la polygon_elevation_ref trabaja mejor
def polygon_elevation_optimizada(geometry, format_out, dataset):
    """
    Performs PostGIS query to enrich a polygon geometry.
    
    :param geometry: Input 2D polygon to be enriched with elevation
    :type geometry: Shapely geometry
    
    :param format_out: Specifies output format. One of ['geojson', 'polygon']
    :type format_out: string
    
    :param dataset: Elevation dataset to use for querying
    :type dataset: string
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description. 
        
    :returns: 3D polygon as GeoJSON or WKT
    :rtype: string
    """
    print('Entrando a poligon_elevation Optimized')

    Model = _getModel(dataset)
    
    if geometry.geom_type == 'Polygon':
        # Convert geometry to SRID 4326 directly
        query_geom = func.ST_SetSRID(
            func.ST_PolygonFromText(geometry.wkt), 4326).label('geom')

        # Use CTE (Common Table Expressions) for better query management
        query_geom_cte = db.get_session() \
                           .query(query_geom) \
                           .cte('query_geom_cte')

        # Combine and optimize pixel extraction and clipping
        result_pixels = db.get_session() \
                            .query(func.ST_PixelAsCentroids(
                                func.ST_Clip(Model.rast, query_geom_cte.c.geom, NO_DATA_VALUE),
                                1, False).label('pixel_centroids')) \
                            .filter(func.ST_Intersects(Model.rast, query_geom_cte.c.geom)) \
                            .distinct() \
                            .all()

        point_col, height_col = format_PixelAsGeoms(result_pixels)

        # Directly create 3D points
        raster_points3d = db.get_session() \
                            .query(func.ST_SetSRID(
                                func.ST_MakePoint(func.ST_X(point_col),
                                                  func.ST_Y(point_col),
                                                  height_col), 4326).label('geom')) \
                            .order_by(func.ST_X(point_col), func.ST_Y(point_col)) \
                            .cte('raster_points3d')

        # Optimize join and filtering for points within the polygon
        query_points3d = db.get_session() \
                            .query(raster_points3d.c.geom) \
                            .filter(func.ST_Within(raster_points3d.c.geom, query_geom_cte.c.geom)) \
                            .cte('points3d')

        if format_out == 'geojson':
            # Directly return GeoJSON
            query_final = db.get_session() \
                            .query(func.ST_AsGeoJson(func.ST_Collect(query_points3d.c.geom)))
        else:
            # Return the WKT of the geometry
            query_final = db.get_session() \
                            .query(func.ST_AsText(func.ST_MakeLine(query_points3d.c.geom)))

    else:
        raise InvalidUsage(400, 4002, f"Needs to be a Polygon, not a {geometry.geom_type}!")
    
    result_geom = query_final.scalar()

    if result_geom is None:
        raise InvalidUsage(404, 4002,
                           f'The requested geometry is outside the bounds of {dataset}')
        
    return result_geom
#AAOR-->Fin poligon_elevation optimizada

#AAOR-->Función process_coordinates
def process_coordinates(result_pixels):
    """
    Procesa una lista de resultados y devuelve una cadena que representa un LINESTRING Z
    y el objeto LineString de Shapely con las coordenadas de longitud, latitud y altura válidas.

    :param result_pixels: Lista de tuplas, donde cada tupla contiene una cadena con datos WKB y altura.
    :return: Una cadena que representa un LINESTRING Z y el objeto LineString de Shapely.
    """
    coordinates = []

    # Procesar cada resultado
    for entry in result_pixels:
        # Separar los componentes de la cadena
        raw_data = entry[0].strip('()').split(',')
        
        # Extraer el WKB, convertir a entero la altura y validar
        hex_wkb = raw_data[0]
        height = int(raw_data[1])
        
        if height != -32768:  # Filtrar alturas no válidas
            # Convertir el WKB hexadecimal a un objeto de geometría de Shapely
            geom = wkb.loads(bytes.fromhex(hex_wkb))
            
            # Obtener las coordenadas de longitud (X) y latitud (Y)
            longitude, latitude = geom.x, geom.y
            
            # Almacenar las coordenadas y la altura válidas en la lista
            coordinates.append((longitude, latitude, height))

    # Crear el objeto LineString Z con las coordenadas válidas
    linestring_z = geometry.LineString(coordinates)

    # Retornar la cadena y el objeto LineString
    return linestring_z.wkt


def process_coordinates_(result_pixels):
    """
    Procesa una lista de resultados y devuelve una cadena que representa un LINESTRING Z
    y el objeto LineString de Shapely con las coordenadas de longitud, latitud y altura válidas.

    :param result_pixels: Lista de tuplas, donde cada tupla contiene una cadena con datos WKB y altura.
    :return: Una cadena que representa un LINESTRING Z y el objeto LineString de Shapely.
    """
    coordinates = []

    # Iniciar temporizador para medir el tiempo de ejecución
    start_time = time.perf_counter()

    # Procesar cada resultado
    for entry in result_pixels:
        # Separar los componentes de la cadena
        raw_data = entry[0].strip('()').split(',')
        
        # Extraer el WKB, convertir a entero la altura y validar
        hex_wkb = raw_data[0]
        height = int(raw_data[1])
        
        if height != -32768:  # Filtrar alturas no válidas
            # Convertir el WKB hexadecimal a un objeto de geometría de Shapely
            geom = wkb.loads(bytes.fromhex(hex_wkb))
            
            # Obtener las coordenadas de longitud (X) y latitud (Y)
            longitude, latitude = geom.x, geom.y
            
            # Almacenar las coordenadas y la altura válidas en la lista
            coordinates.append((longitude, latitude, height))

    # Crear el objeto LineString Z con las coordenadas válidas
    linestring_z = geometry.LineString(coordinates)

    # Terminar temporizador para medir el tiempo de ejecución
    end_time = time.perf_counter()
    print(f"Tiempo de ejecución de process_coordinates_: {end_time - start_time:.6f} segundos")
    
    # Limpiar referencias explícitamente
    del coordinates, result_pixels
    
    # Forzar la recolección de basura para liberar memoria
    gc.collect()

    # Retornar la representación WKT del objeto LineString
    return linestring_z.wkt



#Fin AAOR-->Función process_coordinates



#OJO
#AAOR-->poligon_elevation original
def polygon_elevation(geometry, format_out, dataset):
    """
    Performs PostGIS query to enrich a polygon geometry.
    
    :param geometry: Input 2D polygon to be enriched with elevation
    :type geometry: Shapely geometry
    
    :param format_out: Specifies output format. One of ['geojson', 'polygon']
    :type format_out: string
    
    :param dataset: Elevation dataset to use for querying
    :type dataset: string
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description. 
        
    :returns: 3D polygon as GeoJSON or WKT
    :rtype: string
    """
    
    #Model = _getModel(dataset)
    
    if geometry.geom_type == 'Polygon':

        # Abrir la sesión una sola vez
        session = db.get_session()

        inicio_qgeom = time.perf_counter() #AAOR
        
        #-------

        # Consulta SQL para obtener los puntos y alturas dentro del polígono
        # consulta_sql = """
        #     WITH polygon_geom AS (
        #         SELECT ST_SetSRID(
        #                 ST_GeomFromText(:wkt_poligono), 4326
        #             ) AS polygon
        #     )
        #     SELECT 
        #         ST_X(geom) AS x,
        #         ST_Y(geom) AS y,
        #         ST_Value(rast, 1, geom) AS z
        #     FROM oes_cgiar, polygon_geom,
        #     LATERAL ST_PixelAsCentroids(
        #         ST_Clip(rast, polygon_geom.polygon), 1
        #     ) AS geom
        #     WHERE ST_Intersects(rast, polygon_geom.polygon)
        #     ORDER BY ST_X(geom), ST_Y(geom)
        # """

        # consulta_sql = """
        #     WITH geom_polygon AS (
        #         SELECT ST_SetSRID(
        #             ST_GeomFromText(:wkt_poligono), 4326
        #             ) AS geom
        #     ),
        #     intersecting_rasters AS (
        #         SELECT r.rast, gp.geom
        #         FROM cgiar_data r, geom_polygon gp
        #         WHERE ST_Intersects(r.rast, gp.geom)
        #     ),
        #     clipped_rasters AS (
        #         SELECT ST_Clip(ir.rast, ir.geom) AS clipped_rast
        #         FROM intersecting_rasters ir
        #     ),
        #     pixel_geometries AS (
        #         SELECT (ST_PixelAsPoints(cr.clipped_rast)).geom AS pixel_geom, (ST_PixelAsPoints(cr.clipped_rast)).val AS pixel_value
        #         FROM clipped_rasters cr
        #     )
        #     SELECT 
        #         ST_X(pg.pixel_geom) AS longitud,
        #         ST_Y(pg.pixel_geom) AS latitud,
        #         pg.pixel_value AS altura
        #     FROM pixel_geometries pg, geom_polygon gp
        #     WHERE ST_Covers(gp.geom, pg.pixel_geom);
        # """

        # consulta_sql = """
        #     WITH polygon_geom AS (
        #         SELECT ST_SetSRID(
        #                 ST_GeomFromText(:wkt_poligono), 4326
        #             ) AS polygon
        #     ),
        #     intersecting_rasters AS (
        #         SELECT r.rast, pg.polygon
        #         FROM cgiar_data r, polygon_geom pg
        #         WHERE ST_Intersects(r.rast, pg.polygon)
        #     ),
        #     clipped_rasters AS (
        #         SELECT ST_Clip(ir.rast, ir.polygon) AS clipped_rast
        #         FROM intersecting_rasters ir
        #     ),
        #     pixel_geometries AS (
        #         SELECT (ST_PixelAsPoints(cr.clipped_rast)).geom AS pixel_geom, (ST_PixelAsPoints(cr.clipped_rast)).val AS pixel_value
        #         FROM clipped_rasters cr
        #     )
        #     SELECT 
        #         ST_X(pg.pixel_geom) AS x,
        #         ST_Y(pg.pixel_geom) AS y,
        #         pg.pixel_value AS z
        #     FROM pixel_geometries pg, polygon_geom pg_geom
        #     WHERE ST_Covers(pg_geom.polygon, pg.pixel_geom)
        #     ORDER BY ST_X(pg.pixel_geom), ST_Y(pg.pixel_geom);
        # """

        consulta_sql = """
            WITH polygon_geom AS (
                SELECT ST_SetSRID(
                        ST_GeomFromText(:wkt_poligono), 4326
                    ) AS polygon
            ),
            intersecting_rasters AS (
                SELECT r.rast, pg.polygon
                FROM oes_cgiar r, polygon_geom pg
                WHERE ST_Intersects(r.rast, pg.polygon)
            ),
            clipped_rasters AS (
                SELECT ST_Clip(ir.rast, ir.polygon) AS clipped_rast
                FROM intersecting_rasters ir
            ),
            pixel_geometries AS (
                SELECT (ST_PixelAsPoints(cr.clipped_rast)).geom AS pixel_geom, (ST_PixelAsPoints(cr.clipped_rast)).val AS pixel_value
                FROM clipped_rasters cr
            )
            SELECT 
                ST_X(pg.pixel_geom) AS x,
                ST_Y(pg.pixel_geom) AS y,
                pg.pixel_value AS z
            FROM pixel_geometries pg, polygon_geom pg_geom
            WHERE ST_Covers(pg_geom.polygon, pg.pixel_geom);
        """
        #ORDER BY ST_X(pg.pixel_geom), ST_Y(pg.pixel_geom);-->medir tiempo con order by y sin order by


        def ejecutar_consulta(geometry):
            # Ejecutar la consulta SQL con el WKT del polígono proporcionado
            result_points = session.execute(text(consulta_sql), {"wkt_poligono": geometry.wkt}).fetchall()
            print('lo que llega desde la BBDD')
            print('result_points',type(result_points))
            print('result_pointsssssss',result_points[0],result_points[1],result_points[2],result_points[3])

            # Construir la LINESTRING Z
            linestring_z = "LINESTRING Z ("
            for point in result_points:
                x = point.x
                y = point.y
                z = point.z
                linestring_z += f"{x} {y} {z},"
            
            # Eliminar la última coma y cerrar el paréntesis
            if linestring_z.endswith(','):
                linestring_z = linestring_z[:-1] + ")"

            print('linestring_z',type(linestring_z))
            print('linestring_z',linestring_z[0:160])

            return linestring_z
            
           
        

        # Ejecutar la consulta con la geometría proporcionada
        result_geom=ejecutar_consulta(geometry)




        #-------
        fin_qgeom = time.perf_counter() #AAOR



        tiempo_transcurrido_qgeom = fin_qgeom - inicio_qgeom#AAOR
        print(f"Tiempo de ejecución_qgeom: {tiempo_transcurrido_qgeom:.6f} segundos")#AAOR
       
        
        
    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))

    
    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
    print('----------- ')
    #Versión inicial del código sql-optimizada sin modificaciones en querybuilder_sql_optimizada_v04072024.py
    print('cooooon mejoras sql-optimizada-modificada')
        
    return result_geom
#OJO
##AAOR-->Fin polygon_elevation original


#OJO
##AAOR-->polygon_elevation original con medicion de tiempo
def polygon_elevation__(geometry, format_out, dataset):
    """
    Performs PostGIS query to enrich a polygon geometry.
    
    :param geometry: Input 2D polygon to be enriched with elevation
    :type geometry: Shapely geometry
    
    :param format_out: Specifies output format. One of ['geojson', 'polygon']
    :type format_out: string
    
    :param dataset: Elevation dataset to use for querying
    :type dataset: string
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description. 
        
    :returns: 3D polygon as GeoJSON or WKT
    :rtype: string
    """
    #AAOR
    print('Entrando a poligon_elevation Original con medicion de tiempo')
    Model = _getModel(dataset)
    
    if geometry.geom_type == 'Polygon':

        inicio_consulta_queried_1 = time.perf_counter()#AAOR 

        query_geom = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326) \
                            .label('geom')) \
                            .subquery().alias('pGeom')
        
        fin_consulta_queried_1 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_1 = fin_consulta_queried_1 - inicio_consulta_queried_1#AAOR
        print(f"Tiempo de ejecución_consulta_queried_1: {tiempo_transcurrido_consulta_queried_1:.6f} segundos")#AAOR
        #print('Tiempo de ejecución_consulta',query_geom)#AAOR
        print('Tiempo de ejecución_consulta_queried_1_T',type(query_geom))#AAOR

        print('query_geom')
        #print(query_geom)
        print('\n')#AAOR


        inicio_consulta_queried_2 = time.perf_counter()#AAOR 

        result_pixels = db.get_session() \
                            .query(func.DISTINCT(func.ST_PixelAsCentroids(
                                func.ST_Clip(Model.rast, query_geom.c.geom, NO_DATA_VALUE),
                                1, False))) \
                            .select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))) \
                            .all()
        
        fin_consulta_queried_2 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_2 = fin_consulta_queried_2 - inicio_consulta_queried_2#AAOR
        print(f"Tiempo de ejecución_consulta_queried_2: {tiempo_transcurrido_consulta_queried_2:.6f} segundos")#AAOR
        #print('Tiempo de ejecución_consulta_1',result_pixels)#AAOR
        print('Tiempo de ejecución_consulta_queried_2_T',type(result_pixels))#AAOR

        print('result_pixels')
        #print(result_pixels)
        
        print('\n')#AAOR
        
        inicio_consulta_queried_3 = time.perf_counter()#AAOR 

        point_col, height_col = format_PixelAsGeoms(result_pixels)

        fin_consulta_queried_3 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_3 = fin_consulta_queried_3 - inicio_consulta_queried_3#AAOR
        print(f"Tiempo de ejecución_consulta_queried_3: {tiempo_transcurrido_consulta_queried_3:.6f} segundos")#AAOR
        #print('Tiempo de ejecución_consulta_queried_3',result_pixels)#AAOR
        #print('Tiempo de ejecución_consulta_queried_3_T',type(result_pixels))#AAOR

        print('point_col')
        #print(point_col)

        print('height_col')
        #print(height_col)

        print('\n')#AAOR

        inicio_consulta_queried_4 = time.perf_counter()#AAOR 

        raster_points3d = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_MakePoint(ST_X(point_col),
                                                                     ST_Y(point_col),
                                                                     height_col),
                                              4326).label('geom')) \
                            .order_by(ST_X(point_col), ST_Y(point_col)) \
                            .subquery().alias('raster3d')
        
        fin_consulta_queried_4 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_4 = fin_consulta_queried_4 - inicio_consulta_queried_4#AAOR
        print(f"Tiempo de ejecución_consulta_queried_4: {tiempo_transcurrido_consulta_queried_4:.6f} segundos")#AAOR
        #print('Tiempo de ejecución_consulta_queried_4',type(result_pixels))#AAOR
        print('raster_points3d')
        #print(raster_points3d)


        print('\n')#AAOR

        inicio_consulta_queried_5 = time.perf_counter()#AAOR 

        query_points3d = db.get_session() \
                            .query(raster_points3d.c.geom) \
                            .select_from(raster_points3d) \
                            .join(query_geom, func.ST_Within(raster_points3d.c.geom, query_geom.c.geom)) \
                            .subquery().alias('points3d')
        
        fin_consulta_queried_5 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_5 = fin_consulta_queried_5 - inicio_consulta_queried_5#AAOR
        print(f"Tiempo de ejecución_consulta_queried_5: {tiempo_transcurrido_consulta_queried_5:.6f} segundos")#AAOR
        #print('Tiempo de ejecución_consulta_queried_5',type(result_pixels))#AAOR

        print('query_points3d')
        #print(query_points3d)
        
        print('\n')#AAOR

        if format_out == 'geojson':
            # Return GeoJSON directly in PostGIS
            inicio_consulta_queried_6 = time.perf_counter()#AAOR 

            query_final = db.get_session() \
                              .query(func.ST_AsGeoJson(func.ST_Collect(query_points3d.c.geom)))
            
            fin_consulta_queried_6 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_6 = fin_consulta_queried_6 - inicio_consulta_queried_6#AAOR
            print(f"Tiempo de ejecución_consulta_queried_6: {tiempo_transcurrido_consulta_queried_6:.6f} segundos")#AAOR
            #print('Tiempo de ejecución_consulta_queried_6',type(result_pixels))#AAOR

            print('query_final_IF')
            #print(query_final)
            print('\n')#AAOR
            
        else:
            # Else return the WKT of the geometry
            inicio_consulta_queried_7 = time.perf_counter()#AAOR 

            query_final = db.get_session() \
                              .query(func.ST_AsText(func.ST_MakeLine(query_points3d.c.geom)))
            
            fin_consulta_queried_7 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_7 = fin_consulta_queried_7 - inicio_consulta_queried_7#AAOR
            print(f"Tiempo de ejecución_consulta_queried_7: {tiempo_transcurrido_consulta_queried_7:.6f} segundos")#AAOR
            #print('Tiempo de ejecución_consulta_queried_7',type(result_pixels))#AAOR

            print('query_final_else')
            #print(query_final)
            print('\n')#AAOR

    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))
    
    inicio_consulta_queried_8 = time.perf_counter()#AAOR 

    result_geom = query_final.scalar()

    fin_consulta_queried_8 = time.perf_counter()#AAOR
    tiempo_transcurrido_consulta_queried_8 = fin_consulta_queried_8 - inicio_consulta_queried_8#AAOR
    print(f"Tiempo de ejecución_consulta_queried_8: {tiempo_transcurrido_consulta_queried_8:.6f} segundos")#AAOR
    #print('Tiempo de ejecución_consulta_queried_8',type(result_pixels))#AAOR

    print('result_geom')
    #print(result_geom)
    print('\n')#AAOR

    #OJO:no entra ala 6-->tiempo_transcurrido_consulta_queried_6
    tiempo_transcurrido_consulta_queried_desde_1_hasta_8=tiempo_transcurrido_consulta_queried_1+\
                                                         tiempo_transcurrido_consulta_queried_2+\
                                                         tiempo_transcurrido_consulta_queried_3+\
                                                         tiempo_transcurrido_consulta_queried_4+\
                                                         tiempo_transcurrido_consulta_queried_5+\
                                                         tiempo_transcurrido_consulta_queried_7+\
                                                         tiempo_transcurrido_consulta_queried_8                                                        
    print(f"Tiempo de ejecución_consulta_queried_desde_1_hasta_8: {tiempo_transcurrido_consulta_queried_desde_1_hasta_8:.6f} segundos")#AAOR
    #print('Tiempo de ejecución_consulta_queried_8',type(result_geom))#AAOR
    print('\n')#AAOR

    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_geom
#OJO
##AAOR-->Fin polygon_elevation original con medicion de tiempo

#OJO
#AAOR-->polygon_elevation refactorizada
def polygon_elevation_ref(geometry, format_out, dataset):

    #AAOR
    print('Entrando a poligon_elevation refactorizada')
   
    if geometry.geom_type != 'Polygon':
        raise ValueError("Needs to be a Polygon, not {}!".format(geometry.geom_type))

    session = db.get_session()

    query_geom = session.query(
        func.ST_SetSRID(func.ST_GeomFromText(geometry.wkt), 4326).label('geom')
    ).subquery().alias('pGeom')

    Model = _getModel(dataset)

    inicio_consulta_queried_2 = time.perf_counter()#AAOR 

    result_pixels = session.query(
        func.DISTINCT(func.ST_PixelAsCentroids(
            func.ST_Clip(Model.rast, query_geom.c.geom, NO_DATA_VALUE), 1, False
        ))
    ).select_from(query_geom.join(Model, func.ST_Intersects(Model.rast, query_geom.c.geom))).all()

    fin_consulta_queried_2 = time.perf_counter()#AAOR
    tiempo_transcurrido_consulta_queried_2 = fin_consulta_queried_2 - inicio_consulta_queried_2#AAOR
    print(f"Tiempo de ejecución_consulta_queried_2: {tiempo_transcurrido_consulta_queried_2:.6f} segundos")#AAOR
    #print('Tiempo de ejecución_consulta_1',result_pixels)#AAOR
    #print('Tiempo de ejecución_consulta_queried_2_T',type(result_pixels))#AAOR


    point_col, height_col = format_PixelAsGeoms(result_pixels)

    raster_points3d = session.query(
        func.ST_SetSRID(func.ST_MakePoint(
            func.ST_X(point_col), func.ST_Y(point_col), height_col), 4326).label('geom')
    ).order_by(func.ST_X(point_col), func.ST_Y(point_col)).subquery().alias('raster3d')

    query_points3d = session.query(raster_points3d.c.geom).select_from(raster_points3d).join(
        query_geom, func.ST_Within(raster_points3d.c.geom, query_geom.c.geom)
    ).subquery().alias('points3d')

    if format_out == 'geojson':
        query_final = session.query(func.ST_AsGeoJson(func.ST_Collect(query_points3d.c.geom)))
    else:
        query_final = session.query(func.ST_AsText(func.ST_MakeLine(query_points3d.c.geom)))

    
    inicio_consulta_queried_8 = time.perf_counter()#AAOR 

    result_geom = query_final.scalar()

    fin_consulta_queried_8 = time.perf_counter()#AAOR
    tiempo_transcurrido_consulta_queried_8 = fin_consulta_queried_8 - inicio_consulta_queried_8#AAOR
    print(f"Tiempo de ejecución_consulta_queried_8: {tiempo_transcurrido_consulta_queried_8:.6f} segundos")#AAOR
    #print('result_geom')
    #print(result_geom)
    print('\n')#AAOR




    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_geom
#OJO
#AAOR-->Fin polygon_elevation refactorizada


#OJO--inicia otra
#AAOR-->polygon_elevation refactorizada sin medicion de tiempo(_sin_mt)
def polygon_elevation_ref_sin_mt(geometry, format_out, dataset):
    print('Entrando a poligon_elevation refactorizada sinnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn')
    if geometry.geom_type != 'Polygon':
        raise ValueError(f"Needs to be a Polygon, not {geometry.geom_type}!")

    session = db.get_session()

    # Predefine SRID and avoid multiple function calls
    
    geom_srid = func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326)

    # Use a single query to minimize round trips and simplify logic
    Model = _getModel(dataset)  # Assumes this function is efficient

    # Using joins and WHERE clauses instead of subqueries for performance
    # Extract the required data in a single SQL execution
    result_geom = session.query(
        func.ST_AsGeoJSON(func.ST_Collect(
            func.ST_SetSRID(
                func.ST_MakePoint(
                    func.ST_X(func.ST_PixelAsCentroids(func.ST_Clip(Model.rast, geom_srid, NO_DATA_VALUE), 1, False)),
                    func.ST_Y(func.ST_PixelAsCentroids(func.ST_Clip(Model.rast, geom_srid, NO_DATA_VALUE), 1, False)),
                    func.ST_Value(Model.rast, func.ST_Centroid(func.ST_Clip(Model.rast, geom_srid, NO_DATA_VALUE)))
                ),
                4326
            )
        ))
    ).filter(func.ST_Intersects(Model.rast, geom_srid)).scalar()

    # Check if results were found and handle output formats
    if result_geom is None:
        raise InvalidUsage(404, 4002, f'The requested geometry is outside the bounds of {dataset}')
    
    if format_out != 'geojson':
        # Convert to line if necessary
        result_geom = session.query(func.ST_AsText(func.ST_MakeLine(func.ST_GeomFromGeoJSON(result_geom)))).scalar()

    return result_geom


#OJO
#AAOR-->Fin polygon_elevation refactorizada sin medicion de tiempo








def line_elevation(geometry, format_out, dataset):
    """
    Performs PostGIS query to enrich a line geometry.
    
    :param geometry: Input 2D line to be enriched with elevation
    :type geometry: Shapely geometry
    
    :param format_out: Specifies output format. One of ['geojson', 'polyline',
        'encodedpolyline']
    :type format_out: string
    
    :param dataset: Elevation dataset to use for querying
    :type dataset: string
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description. 
        
    :returns: 3D line as GeoJSON or WKT
    :rtype: string
    """
    
    Model = _getModel(dataset)
    
    if geometry.geom_type == 'LineString':
        num_points = db.get_session() \
                        .query(func.ST_NPoints(geometry.wkt)) \
                        .scalar()
        
        if int(num_points) != 2:
            raise InvalidUsage(400, 4002, "Actually, only LineString with exactly 2 points are supported!")

        minX, minY, maxX, maxY = geometry.bounds
        lineLen = ((maxX - minX) ** 2 + (maxY - minY) ** 2) ** 0.5
        

        points_clause = [
            func.ST_PointN(geometry.wkt, 1),
            func.ST_PointN(geometry.wkt, 2)
        ]
        if lineLen != 0:
            points_clause.insert(1, func.ST_LineInterpolatePoints(
                geometry.wkt,
                min(1, coord_precision / lineLen)
            ))

        query_points2d = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_DumpPoints(func.ST_Union(
                                array(points_clause)
                            )).geom, 4326).label('geom')).subquery().alias('points2d')

        query_getelev = db.get_session() \
                            .query(func.DISTINCT(query_points2d.c.geom).label('geom'),
                                   ST_Value(Model.rast, query_points2d.c.geom).label('z')) \
                            .select_from(query_points2d) \
                            .join(Model, ST_Intersects(Model.rast, query_points2d.c.geom)) \
                            .subquery().alias('getelevation')

        query_points3d = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_MakePoint(ST_X(query_getelev.c.geom),
                                                                     ST_Y(query_getelev.c.geom),
                                                                     func.coalesce(query_getelev.c.z, NO_DATA_VALUE)),
                                              4326).label('geom')) \
                            .order_by(func.ST_Distance(
                                query_getelev.c.geom,
                                func.ST_SetSRID(func.ST_PointN(geometry.wkt, 1), 4326)
                            )) \
                            .subquery().alias('points3d')
                            

        if format_out == 'geojson':
            # Return GeoJSON directly in PostGIS
            query_final = db.get_session() \
                              .query(func.ST_AsGeoJson(func.ST_MakeLine(query_points3d.c.geom))) #ST_SnapToGrid(, coord_precision)
            
        else:
            # Else return the WKT of the geometry
            query_final = db.get_session() \
                              .query(func.ST_AsText(func.ST_MakeLine(query_points3d.c.geom))) #ST_SnapToGrid(, coord_precision)
    else:
        raise InvalidUsage(400, 4002, "Needs to be a LineString, not a {}!".format(geometry.geom_type))

    result_geom = query_final.scalar()

    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_geom




@medir_tiempo#AAOR
def point_elevation(geometry, format_out, dataset):
    """
    Performs PostGIS query to enrich a point geometry.
    
    :param geometry: Input point to be enriched with elevation
    :type geometry: shapely.geometry.Point
    
    :param format_out: Specifies output format. One of ['geojson', 'point']
    :type format_out: string
    
    :param dataset: Elevation dataset to use for querying
    :type dataset: string
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description.
    
    :returns: 3D Point as GeoJSON or WKT
    :rtype: string
    """
    
    Model = _getModel(dataset)
    
    if geometry.geom_type == "Point":
        query_point2d = db.get_session() \
                            .query(func.ST_SetSRID(func.St_PointFromText(geometry.wkt), 4326).label('geom')) \
                            .subquery() \
                            .alias('points2d')
        
        query_getelev = db.get_session() \
                            .query(query_point2d.c.geom,
                                   ST_Value(Model.rast, query_point2d.c.geom).label('z')) \
                            .select_from(query_point2d) \
                            .join(Model, ST_Intersects(Model.rast, query_point2d.c.geom)) \
                            .limit(1) \
                            .subquery().alias('getelevation')
        
        if format_out == 'geojson': 
            query_final = db.get_session() \
                                .query(func.ST_AsGeoJSON(func.ST_MakePoint(ST_X(query_getelev.c.geom),
                                                                           ST_Y(query_getelev.c.geom),
                                                                           func.coalesce(query_getelev.c.z, NO_DATA_VALUE)
                                                                        )))
        else:
            query_final = db.get_session() \
                                .query(func.ST_AsText(func.ST_MakePoint(ST_X(query_getelev.c.geom),
                                                                        ST_Y(query_getelev.c.geom),
                                                                        func.coalesce(query_getelev.c.z, NO_DATA_VALUE)
                                                                    )))
    else:
        raise InvalidUsage(400, 4002, "Needs to be a Point, not {}!".format(geometry.geom_type))
    
    result_geom = query_final.scalar()

    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_geom
