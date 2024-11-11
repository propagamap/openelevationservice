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

#AAOR-Import
import time
import json
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.ops import unary_union
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor, as_completed
import math
#AAOR-Fin Import

log = get_logger(__name__)

coord_precision = float(SETTINGS['coord_precision'])

NO_DATA_VALUE = -32768

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


###Start-Original code for the polygon_coloring_elevation function
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

    print("AreaRangesElevation-originallll")
    #print("geometry",geometry)

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
    #print("desde original",result_geom)
    
    return result_geom, [min_height, max_height], avg_height

##End-Original code for the polygon_coloring_elevation function

##Start-Original - cmt - AAOR code for polygon_coloring_elevation function
def polygon_coloring_elevation_modified_cmt(geometry, dataset):
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
    inicio_consulta_queried_0 = time.perf_counter()#AAOR
    Model = _getModel(dataset)

    print("AreaRangesElevation-originallll-modified - cmt")
    fin_consulta_queried_0 = time.perf_counter()#AAOR
    tiempo_transcurrido_consulta_queried_0 = fin_consulta_queried_0 - inicio_consulta_queried_0#AAOR
    print(f"Tiempo de ejecución_consulta_queried_0: {tiempo_transcurrido_consulta_queried_0:.6f} segundos")#AAOR
    print(" ")

    num_ranges = 23
    
    if geometry.geom_type == 'Polygon':

        inicio_consulta_queried_1 = time.perf_counter()#AAOR 
        query_geom = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326) \
                            .label('geom')) \
                            .subquery().alias('pGeom')
        
        fin_consulta_queried_1 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_1 = fin_consulta_queried_1 - inicio_consulta_queried_1#AAOR
        print(f"Tiempo de ejecución_consulta_queried_1: {tiempo_transcurrido_consulta_queried_1:.6f} segundos")#AAOR
        #print("query_geom",query_geom)
        print(" ")

        inicio_consulta_queried_2 = time.perf_counter()#AAOR 
        result_pixels = db.get_session() \
                            .query(func.DISTINCT(func.ST_PixelAsPolygons(
                                func.ST_Clip(Model.rast, query_geom.c.geom, NO_DATA_VALUE),
                                1, False))) \
                            .select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))) \
                            .all()
        fin_consulta_queried_2 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_2 = fin_consulta_queried_2 - inicio_consulta_queried_2#AAOR
        print(f"Tiempo de ejecución_consulta_queried_2: {tiempo_transcurrido_consulta_queried_2:.6f} segundos")#AAOR
        #print("result_pixels",result_pixels)
        print(" ")

        #OJOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
        # Inicializamos el contador para los valores -32768
        contador_negativos = 0

        # Recorrer los píxeles originales para extraer valores y filtrar duplicados
        for pixel in result_pixels:
            # Eliminar los paréntesis y dividir por comas
            parts = pixel[0].strip('()').split(',')
            
            # Extraer los valores 1, 2, 3 y 4
            val1 = parts[0]  # Valor en la posición 1 (WKT text)
            val2 = int(parts[1].strip())  # Convertir a entero el valor en la posición 2 (número entero)
            
            # Verificar si el valor de altura es igual a -32768
            if val2 == -32768:
                contador_negativos += 1  # Aumentar el contador
                continue  # Saltar este pixel y continuar con el siguiente
                
            val3 = int(parts[2].strip())  # Valor en la posición 3 (número entero)
            val4 = int(parts[3].strip())  # Valor en la posición 4 (número entero)

        # Mostrar cuántos valores -32768 se encontraron
        print(f"Se encontraron {contador_negativos} valores de -32768.")
        #Fin OJOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
        
        
        inicio_consulta_queried_3 = time.perf_counter()#AAOR 
        #polygon_col, height_col = format_PixelAsGeoms(resultado_final)
        polygon_col, height_col = format_PixelAsGeoms(result_pixels)
        fin_consulta_queried_3 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_3 = fin_consulta_queried_3 - inicio_consulta_queried_3#AAOR
        print(f"Tiempo de ejecución_consulta_queried_3: {tiempo_transcurrido_consulta_queried_3:.6f} segundos")#AAOR
        #print("polygon_col",polygon_col)
        print(" ")
        print("height_col",height_col)
        print(" ")

        inicio_consulta_queried_4 = time.perf_counter()#AAOR 
        rebuilt_set = db.get_session() \
                            .query(polygon_col.label("geometry"),
                                   height_col.label("height")) \
                            .subquery().alias('rebuiltSet')
        fin_consulta_queried_4 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_4 = fin_consulta_queried_4 - inicio_consulta_queried_4#AAOR
        print(f"Tiempo de ejecución_consulta_queried_4: {tiempo_transcurrido_consulta_queried_4:.6f} segundos")#AAOR
        #print("rebuilt_set",rebuilt_set)
        print(" ")

        inicio_consulta_queried_5 = time.perf_counter()#AAOR 
        filtered_set = db.get_session() \
                            .query(rebuilt_set.c.geometry,
                                   rebuilt_set.c.height) \
                            .select_from(rebuilt_set) \
                            .join(query_geom, func.ST_Within(func.ST_Centroid(rebuilt_set.c.geometry), query_geom.c.geom)) \
                            .subquery().alias('filteredSet')
        fin_consulta_queried_5 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_5 = fin_consulta_queried_5 - inicio_consulta_queried_5#AAOR
        print(f"Tiempo de ejecución_consulta_queried_5: {tiempo_transcurrido_consulta_queried_5:.6f} segundos")#AAOR
        #print("rebuilt_set",rebuilt_set)
        print(" ")
        
        inicio_consulta_queried_6 = time.perf_counter()#AAOR 
        min_height, max_height, avg_height = db.get_session() \
                            .query(func.min(filtered_set.c.height),
                                   func.max(filtered_set.c.height),
                                   func.avg(filtered_set.c.height)) \
                            .select_from(filtered_set) \
                            .where(filtered_set.c.height != NO_DATA_VALUE) \
                            .one()
        fin_consulta_queried_6 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_6 = fin_consulta_queried_6 - inicio_consulta_queried_6#AAOR
        print(f"Tiempo de ejecución_consulta_queried_6: {tiempo_transcurrido_consulta_queried_6:.6f} segundos")#AAOR
        #print("min_height",min_height)
        print(" ")
        #print("max_height",max_height)
        #print(" ")
        #print("avg_height",avg_height)
        #print(" ")
        
        
        if min_height is None or max_height is None:
            raise InvalidUsage(400, 4002,
                               'The requested geometry does not contain any elevation data')
        else:
            inicio_consulta_queried_7 = time.perf_counter()#AAOR 
            range_div = (max_height - min_height + 1) / num_ranges
            fin_consulta_queried_7 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_7 = fin_consulta_queried_7 - inicio_consulta_queried_7#AAOR
            print(f"Tiempo de ejecución_consulta_queried_7: {tiempo_transcurrido_consulta_queried_7:.6f} segundos")#AAOR

            
            inicio_consulta_queried_8 = time.perf_counter()#AAOR 
            ranged_set = db.get_session() \
                                .query(filtered_set.c.geometry,
                                    func.GREATEST(
                                        func.LEAST(func.floor((filtered_set.c.height - min_height) / range_div), num_ranges), -1
                                    ).label("colorRange")) \
                                .select_from(filtered_set) \
                                .subquery().alias('rangedSet')
            fin_consulta_queried_8 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_8 = fin_consulta_queried_8 - inicio_consulta_queried_8#AAOR
            print(f"Tiempo de ejecución_consulta_queried_8: {tiempo_transcurrido_consulta_queried_8:.6f} segundos")#AAOR

            inicio_consulta_queried_9 = time.perf_counter()#AAOR 
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
            fin_consulta_queried_9 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_9 = fin_consulta_queried_9 - inicio_consulta_queried_9#AAOR
            print(f"Tiempo de ejecución_consulta_queried_9: {tiempo_transcurrido_consulta_queried_9:.6f} segundos")#AAOR
            print("query_features",query_features)
            print(" ")

            # Return GeoJSON directly in PostGIS
            inicio_consulta_queried_10 = time.perf_counter()#AAOR 
            query_final = db.get_session() \
                                .query(func.jsonb_build_object(
                                    'type', 'FeatureCollection',
                                    'features', func.jsonb_agg(query_features.c.features))) \
                                .select_from(query_features)
            fin_consulta_queried_10 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_10 = fin_consulta_queried_10 - inicio_consulta_queried_10#AAOR
            print(f"Tiempo de ejecución_consulta_queried_10: {tiempo_transcurrido_consulta_queried_10:.6f} segundos")#AAOR
            #print("query_final",query_final)
            print(" ")

    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))
    
    inicio_consulta_queried_11 = time.perf_counter()#AAOR 
    result_geom = query_final.scalar()
    fin_consulta_queried_11 = time.perf_counter()#AAOR
    tiempo_transcurrido_consulta_queried_11 = fin_consulta_queried_11 - inicio_consulta_queried_11#AAOR
    print(f"Tiempo de ejecución_consulta_queried_11: {tiempo_transcurrido_consulta_queried_11:.6f} segundos")#AAOR
    print("result_geom",result_geom)


    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))

    return result_geom, [min_height, max_height], avg_height
##End-Original - cmt - AAOR code for polygon_coloring_elevation function


##Start-Original code for the polygon_coloring_elevation_cmt function
def polygon_coloring_elevation_cmt(geometry, dataset):
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

    print("AreaRangesElevation-original-cmt")

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
        print(" ")
        #print("result_pixels",result_pixels)
        print(" ")
        
        polygon_col, height_col = format_PixelAsGeoms(result_pixels)
        #print("polygon_col \ln",polygon_col)
        print(" ")
        #print("height_col",height_col)
        print(" ")

        rebuilt_set = db.get_session() \
                            .query(polygon_col.label("geometry"),
                                   height_col.label("height")) \
                            .subquery().alias('rebuiltSet')
        
        print("rebuilt_set",rebuilt_set)
        print(" ")

        filtered_set = db.get_session() \
                            .query(rebuilt_set.c.geometry,
                                   rebuilt_set.c.height) \
                            .select_from(rebuilt_set) \
                            .join(query_geom, func.ST_Within(func.ST_Centroid(rebuilt_set.c.geometry), query_geom.c.geom)) \
                            .subquery().alias('filteredSet')
        #print("filtered_set",filtered_set)
        print(" ")

        
        min_height, max_height, avg_height = db.get_session() \
                            .query(func.min(filtered_set.c.height),
                                   func.max(filtered_set.c.height),
                                   func.avg(filtered_set.c.height)) \
                            .select_from(filtered_set) \
                            .where(filtered_set.c.height != NO_DATA_VALUE) \
                            .one()
        
        print("min_height",min_height)
        print(" ")
        print("max_height",max_height)
        print(" ")
        print("avg_height",avg_height)
        print(" ")
        
        if min_height is None or max_height is None:
            raise InvalidUsage(400, 4002,
                               'The requested geometry does not contain any elevation data')
        else:
            range_div = (max_height - min_height + 1) / num_ranges
            #print("range_div",range_div)
            print(" ")

            ranged_set = db.get_session() \
                                .query(filtered_set.c.geometry,
                                    func.GREATEST(
                                        func.LEAST(func.floor((filtered_set.c.height - min_height) / range_div), num_ranges), -1
                                    ).label("colorRange")) \
                                .select_from(filtered_set) \
                                .subquery().alias('rangedSet')
            #print("ranged_set",ranged_set)
            print(" ")


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
            #print("query_features",query_features)
            print(" ")


            # Return GeoJSON directly in PostGIS
            query_final = db.get_session() \
                                .query(func.jsonb_build_object(
                                    'type', 'FeatureCollection',
                                    'features', func.jsonb_agg(query_features.c.features))) \
                                .select_from(query_features)
            
            #print("query_final",query_final)
            print(" ")

    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))
    
    result_geom = query_final.scalar()
    #print("result_geom",result_geom)
    print(" ")

    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))

    return result_geom, [min_height, max_height], avg_height
##End-Original code for the polygon_coloring_elevation_cmt function


##Start-Modified AAOR code for polygon_coloring_elevation function
def polygon_coloring_elevation_modified(geometry, dataset):
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
    inicio_consulta_queried_0 = time.perf_counter()#AAOR
    Model = _getModel(dataset)
    fin_consulta_queried_0 = time.perf_counter()#AAOR
    tiempo_transcurrido_consulta_queried_0 = fin_consulta_queried_0 - inicio_consulta_queried_0#AAOR
    print(f"Tiempo de ejecución_consulta_queried_0: {tiempo_transcurrido_consulta_queried_0:.6f} segundos")#AAOR

    num_ranges = 23
    
    if geometry.geom_type == 'Polygon':

        inicio_consulta_queried_1 = time.perf_counter()#AAOR 
        query_geom = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326) \
                            .label('geom')) \
                            .subquery().alias('pGeom')
        
        fin_consulta_queried_1 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_1 = fin_consulta_queried_1 - inicio_consulta_queried_1#AAOR
        print(f"Tiempo de ejecución_consulta_queried_1: {tiempo_transcurrido_consulta_queried_1:.6f} segundos")#AAOR

        inicio_consulta_queried_2 = time.perf_counter()#AAOR 
        result_pixels = db.get_session() \
                            .query(func.DISTINCT(func.ST_PixelAsPolygons(
                                func.ST_Clip(Model.rast, query_geom.c.geom, NO_DATA_VALUE),
                                1, False))) \
                            .select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))) \
                            .all()
        print("result_pixels",result_pixels)
        fin_consulta_queried_2 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_2 = fin_consulta_queried_2 - inicio_consulta_queried_2#AAOR
        print(f"Tiempo de ejecución_consulta_queried_2: {tiempo_transcurrido_consulta_queried_2:.6f} segundos")#AAOR
        
        inicio_consulta_queried_3 = time.perf_counter()#AAOR 
        polygon_col, height_col = format_PixelAsGeoms(result_pixels)
        fin_consulta_queried_3 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_3 = fin_consulta_queried_3 - inicio_consulta_queried_3#AAOR
        print(f"Tiempo de ejecución_consulta_queried_3: {tiempo_transcurrido_consulta_queried_3:.6f} segundos")#AAOR

        inicio_consulta_queried_4 = time.perf_counter()#AAOR 
        rebuilt_set = db.get_session() \
                            .query(polygon_col.label("geometry"),
                                   height_col.label("height")) \
                            .subquery().alias('rebuiltSet')
        fin_consulta_queried_4 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_4 = fin_consulta_queried_4 - inicio_consulta_queried_4#AAOR
        print(f"Tiempo de ejecución_consulta_queried_4: {tiempo_transcurrido_consulta_queried_4:.6f} segundos")#AAOR

        inicio_consulta_queried_5 = time.perf_counter()#AAOR 
        filtered_set = db.get_session() \
                            .query(rebuilt_set.c.geometry,
                                   rebuilt_set.c.height) \
                            .select_from(rebuilt_set) \
                            .join(query_geom, func.ST_Within(func.ST_Centroid(rebuilt_set.c.geometry), query_geom.c.geom)) \
                            .subquery().alias('filteredSet')
        fin_consulta_queried_5 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_5 = fin_consulta_queried_5 - inicio_consulta_queried_5#AAOR
        print(f"Tiempo de ejecución_consulta_queried_5: {tiempo_transcurrido_consulta_queried_5:.6f} segundos")#AAOR
        
        inicio_consulta_queried_6 = time.perf_counter()#AAOR 
        min_height, max_height, avg_height = db.get_session() \
                            .query(func.min(filtered_set.c.height),
                                   func.max(filtered_set.c.height),
                                   func.avg(filtered_set.c.height)) \
                            .select_from(filtered_set) \
                            .where(filtered_set.c.height != NO_DATA_VALUE) \
                            .one()
        fin_consulta_queried_6 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_6 = fin_consulta_queried_6 - inicio_consulta_queried_6#AAOR
        print(f"Tiempo de ejecución_consulta_queried_6: {tiempo_transcurrido_consulta_queried_6:.6f} segundos")#AAOR
        
        if min_height is None or max_height is None:
            raise InvalidUsage(400, 4002,
                               'The requested geometry does not contain any elevation data')
        else:
            inicio_consulta_queried_7 = time.perf_counter()#AAOR 
            range_div = (max_height - min_height + 1) / num_ranges
            fin_consulta_queried_7 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_7 = fin_consulta_queried_7 - inicio_consulta_queried_7#AAOR
            print(f"Tiempo de ejecución_consulta_queried_7: {tiempo_transcurrido_consulta_queried_7:.6f} segundos")#AAOR

            
            inicio_consulta_queried_8 = time.perf_counter()#AAOR 
            ranged_set = db.get_session() \
                                .query(filtered_set.c.geometry,
                                    func.GREATEST(
                                        func.LEAST(func.floor((filtered_set.c.height - min_height) / range_div), num_ranges), -1
                                    ).label("colorRange")) \
                                .select_from(filtered_set) \
                                .subquery().alias('rangedSet')
            fin_consulta_queried_8 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_8 = fin_consulta_queried_8 - inicio_consulta_queried_8#AAOR
            print(f"Tiempo de ejecución_consulta_queried_8: {tiempo_transcurrido_consulta_queried_8:.6f} segundos")#AAOR

            inicio_consulta_queried_9 = time.perf_counter()#AAOR 
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
            fin_consulta_queried_9 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_9 = fin_consulta_queried_9 - inicio_consulta_queried_9#AAOR
            print(f"Tiempo de ejecución_consulta_queried_9: {tiempo_transcurrido_consulta_queried_9:.6f} segundos")#AAOR

            # Return GeoJSON directly in PostGIS
            inicio_consulta_queried_10 = time.perf_counter()#AAOR 
            query_final = db.get_session() \
                                .query(func.jsonb_build_object(
                                    'type', 'FeatureCollection',
                                    'features', func.jsonb_agg(query_features.c.features))) \
                                .select_from(query_features)
            fin_consulta_queried_10 = time.perf_counter()#AAOR
            tiempo_transcurrido_consulta_queried_10 = fin_consulta_queried_10 - inicio_consulta_queried_10#AAOR
            print(f"Tiempo de ejecución_consulta_queried_10: {tiempo_transcurrido_consulta_queried_10:.6f} segundos")#AAOR

    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))
    
    inicio_consulta_queried_11 = time.perf_counter()#AAOR 
    result_geom = query_final.scalar()
    fin_consulta_queried_11 = time.perf_counter()#AAOR
    tiempo_transcurrido_consulta_queried_11 = fin_consulta_queried_11 - inicio_consulta_queried_11#AAOR
    print(f"Tiempo de ejecución_consulta_queried_11: {tiempo_transcurrido_consulta_queried_11:.6f} segundos")#AAOR


    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))

    return result_geom, [min_height, max_height], avg_height
##End-Modified AAOR code for polygon_coloring_elevation function


####Nuevo analisis 22oct2024

#-------------Consulta 6: area grande > 900 km2 (con adyacencia)
# CONSULTA_6 = text(
#     """
#     WITH query_geom AS (
#         -- Polígono de entrada que define la zona de interés
#         SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
#     ),
#     polygons AS (
#         -- Extrae los polígonos (celdas) y el valor de elevación del raster que intersectan con el polígono de entrada
#         SELECT 
#             (ST_PixelAsPolygons(
#                 ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
#                 1, False
#             )).geom AS geometry,  -- Extrae las celdas como geometría
#             (ST_PixelAsPolygons(
#                 ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
#                 1, False
#             )).val AS height  -- Extrae el valor de elevación
#         FROM query_geom 
#         JOIN oes_cgiar 
#         ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
#     ),
#     grouped AS (
#         -- Agrupa los polígonos adyacentes con la misma altura
#         SELECT 
#             height,
#             ST_Union(geometry) AS geometry
#         FROM polygons p1
#         WHERE EXISTS (
#             SELECT 1
#             FROM polygons p2
#             WHERE 
#                 p1.height = p2.height 
#                 AND ST_Touches(p1.geometry, p2.geometry)
#         )
#         GROUP BY height
        
#         UNION ALL  -- Incluye todos los polígonos que no son adyacentes
#         SELECT 
#             height,
#             geometry
#         FROM polygons p1
#         WHERE NOT EXISTS (
#             SELECT 1
#             FROM polygons p2
#             WHERE 
#                 p1.height = p2.height 
#                 AND ST_Touches(p1.geometry, p2.geometry)
#         )
#     )

#     -- Genera el resultado en formato GeoJSON
#     SELECT jsonb_build_object(
#         'type', 'FeatureCollection',
#         'features', jsonb_agg(jsonb_build_object(
#             'type', 'Feature',
#             'geometry', ST_AsGeoJSON(geometry),
#             'properties', json_build_object(
#                 'heightBase', height
#             )
#         ))
#     ) AS features_collection, 
#     MIN(height) AS min_height, 
#     MAX(height) AS max_height, 
#     AVG(height) AS avg_height
#     FROM grouped;
#     """
# )

#####---------->Consulta que no incluye tilas con alturas igual a cero
CONSULTA_6 = text(
    """
    WITH query_geom AS (
    -- Polígono de entrada que define la zona de interés
    SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
),
polygons AS (
    -- Extrae los polígonos (celdas) y el valor de elevación del raster que intersectan con el polígono de entrada
    SELECT 
        (ST_PixelAsPolygons(
            ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
            1, False
        )).geom AS geometry,  -- Extrae las celdas como geometría
        (ST_PixelAsPolygons(
            ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
            1, False
        )).val AS height  -- Extrae el valor de elevación
    FROM query_geom 
    JOIN oes_cgiar 
    ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
),
filtered_polygons AS (
    -- Filtrar los polígonos que tienen altura distinta de cero
    SELECT * 
    FROM polygons 
    WHERE height != 0
),
grouped AS (
    -- Agrupa los polígonos adyacentes con la misma altura
    SELECT 
        height,
        ST_Union(geometry) AS geometry
    FROM filtered_polygons p1
    WHERE EXISTS (
        SELECT 1
        FROM filtered_polygons p2
        WHERE 
            p1.height = p2.height 
            AND ST_Touches(p1.geometry, p2.geometry)
    )
    GROUP BY height
    
    UNION ALL  -- Incluye todos los polígonos que no son adyacentes
    SELECT 
        height,
        geometry
    FROM filtered_polygons p1
    WHERE NOT EXISTS (
        SELECT 1
        FROM filtered_polygons p2
        WHERE 
            p1.height = p2.height 
            AND ST_Touches(p1.geometry, p2.geometry)
    )
)

-- Genera el resultado en formato GeoJSON
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(geometry),
        'properties', json_build_object(
            'heightBase', height
        )
    ))
) AS features_collection, 
MIN(height) AS min_height, 
MAX(height) AS max_height, 
AVG(height) AS avg_height
FROM grouped;

    """
)

# Función para procesar los datos de elevación para un polígono y retornar un objeto JSON
def polygon_coloring_elevation_consulta_6(geometry, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("-------------Consulta 6: área grande > 900 km2 (con adyacencia)")
    
    #Poligono de prueba para un area grande-->mas de 900km2
    #polygon = 'POLYGON((-3.41314 40.4762, -3.289893 40.4762, -3.289893 40.91916, -3.41314 40.91916, -3.41314 40.4762))'
    #print("polygon", polygon)

    #proceso geometry que es el poligono entrante
    #print("polygon", geometry)
    polygon = f"{geometry}"
    print("polygon", polygon)


    # Obtener la sesión de la base de datos
    session = db.get_session()

    # Ejecutar la consulta con el polígono como parámetro
    try:
        inicio = time.perf_counter()
        result = session.execute(CONSULTA_6, {"polygon": polygon})
        fin = time.perf_counter()
        print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")

        # Obtener un solo resultado (dado que estamos esperando un único GeoJSON y estadísticas)
        row = result.fetchone()

        if row:
            # Desempaquetar los resultados: features_collection, min_height, max_height, avg_height
            features_collection, min_height, max_height, avg_height = row
            #features_collection = row

            # Imprimir o retornar los resultados
            #print("GeoJSON Features Collection:", features_collection)

            with open("salida_agrupada_area_20_tilas.json", "w") as archivo:
                json.dump(features_collection, archivo, indent=4) 

            # print("Min Height:", min_height)
            # print("Max Height:", max_height)
            # print("Avg Height:", avg_height)
        else:
            print("No se devolvieron resultados.")

    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")

    # Retornar el objeto en formato JSON
    return features_collection, [min_height, max_height], avg_height
#-------------Fin Consulta 6: area grande > 900 km2 (con adyacencia)


#####--------->Consulta 7: area grande > 900 km2 (sin adyacencia)
# CONSULTA_7 = text(
#     """
#     WITH query_geom AS (
#     -- Polígono de entrada que define la zona de interés
#     SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
# ),
# polygons AS (
#     -- Extrae las celdas raster (polígonos) y el valor de elevación dentro del área de interés
#     SELECT 
#         (ST_PixelAsPolygons(
#             ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
#             1, False
#         )).geom AS geometry,  -- Extrae las celdas como geometría
#         (ST_PixelAsPolygons(
#             ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
#             1, False
#         )).val AS height  -- Extrae el valor de elevación
#     FROM query_geom 
#     JOIN oes_cgiar 
#     ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
# )

# -- Genera el resultado en formato GeoJSON sin agrupar
# SELECT jsonb_build_object(
#     'type', 'FeatureCollection',
#     'features', jsonb_agg(jsonb_build_object(
#         'type', 'Feature',
#         'geometry', ST_AsGeoJSON(geometry),
#         'properties', json_build_object(
#             'heightBase', height
#         )
#     ))
# ) AS features_collection, 
# MIN(height) AS min_height, 
# MAX(height) AS max_height, 
# AVG(height) AS avg_height
# FROM polygons;

#     """
# )

##########Se filtran las tilas con alturas iguales a cero
CONSULTA_7 = text(
    """
    WITH query_geom AS (
    -- Polígono de entrada que define la zona de interés
    SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
),
polygons AS (
    -- Extrae las celdas raster (polígonos) y el valor de elevación dentro del área de interés, luego filtra las alturas diferentes de cero
    SELECT *
    FROM (
        SELECT 
            (ST_PixelAsPolygons(
                ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
                1, False
            )).geom AS geometry,  -- Extrae las celdas como geometría
            (ST_PixelAsPolygons(
                ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
                1, False
            )).val AS height  -- Extrae el valor de elevación
        FROM query_geom 
        JOIN oes_cgiar 
        ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
    ) AS subquery
    WHERE height != 0  -- Filtra las alturas iguales a cero
)

-- Genera el resultado en formato GeoJSON sin agrupar
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(geometry),
        'properties', json_build_object(
            'heightBase', height
        )
    ))
) AS features_collection,
MIN(height) AS min_height,
MAX(height) AS max_height,
AVG(height) AS avg_height
FROM polygons;

    """
)

###--->Función que no_clasifica elevaciones por rango_para pruebas
def no_classify_elevation(features_collection):
         
    classified_features = {
        "type": "FeatureCollection",
        "features": []
    }

    
    for feature in features_collection['features']:
        
        height = feature['properties']['heightBase']
        
        
             
       
              
        classified_feature = {
            "type": "Feature",
            "geometry": json.loads(feature['geometry']),
            "properties": {
                "heightBase": height
            }
        }

        
        classified_features["features"].append(classified_feature)

    return classified_features
###--->Fin Función que no_clasifica elevaciones por rango_para pruebas

###funciones usadas para clasificar elevaciones paralelizando
###libreria multiprocessing
def classify_feature_multiprocessing(feature, min_height, max_height, range_div, no_data_value, num_ranges):
    height = feature['properties']['heightBase']
    
    # Calcular el rango de color según la altura
    color_range = max(
        min(
            math.floor((height - min_height) / range_div),
            num_ranges
        ), -1
    )
    
    # Asignar el valor de heightBase basado en el rango de color o el valor no_data
    if color_range == -1:
        height_base = no_data_value
    else:
        height_base = math.ceil(color_range * range_div + min_height)

    # Crear una nueva feature clasificada
    classified_feature = {
        "type": "Feature",
        "geometry": feature['geometry'],
        "properties": {
            "heightBase": height_base,
            "colorRange": color_range
        }
    }

    return classified_feature

# Función principal que usa paralelización para clasificar todas las features
def classify_elevation_parallel_multiprocessing(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999, num_processes=4, chunk_size=10):
    # Divisor para el cálculo de rangos de color
    range_div = (max_height - min_height + 1) / num_ranges

    # Preparar argumentos para cada feature en el formato esperado
    task_args = [
        (feature, min_height, max_height, range_div, no_data_value, num_ranges)
        for feature in features_collection['features']
    ]

    # Ejecutar la clasificación en paralelo usando Pool
    with Pool(num_processes) as pool:
        classified_features_list = pool.starmap(classify_feature_multiprocessing, task_args, chunksize=chunk_size)

    # Construir el nuevo FeatureCollection con las features clasificadas
    classified_features = {
        "type": "FeatureCollection",
        "features": classified_features_list
    }

    return classified_features

###fin libreria multiprocessing

###Libreria consurrent
#Función de clasificación de elevación para cada feature individual
def classify_feature_consurrent(feature, min_height, max_height, range_div, no_data_value, num_ranges):
    height = feature['properties']['heightBase']
    
    # Calcular el rango de color según la altura
    color_range = max(
        min(
            math.floor((height - min_height) / range_div),
            num_ranges
        ), -1
    )
    
    # Asignar el valor de heightBase basado en el rango de color o el valor no_data
    if color_range == -1:
        height_base = no_data_value
    else:
        height_base = math.ceil(color_range * range_div + min_height)

    # Crear una nueva feature clasificada
    classified_feature = {
        "type": "Feature",
        "geometry": feature['geometry'],
        "properties": {
            "heightBase": height_base,
            "colorRange": color_range
        }
    }

    return classified_feature

# Función principal que usa concurrent.futures para clasificar todas las features
def classify_elevation_parallel_consurrent(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999, num_processes=4):
    # Divisor para el cálculo de rangos de color
    range_div = (max_height - min_height + 1) / num_ranges

    # Preparar argumentos para cada feature en el formato esperado
    task_args = [
        (feature, min_height, max_height, range_div, no_data_value, num_ranges)
        for feature in features_collection['features']
    ]

    # Ejecutar la clasificación en paralelo usando ProcessPoolExecutor
    classified_features_list = []
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = {executor.submit(classify_feature_consurrent, *args): args[0] for args in task_args}
        for future in as_completed(futures):
            result = future.result()
            classified_features_list.append(result)

    # Construir el nuevo FeatureCollection con las features clasificadas
    classified_features = {
        "type": "FeatureCollection",
        "features": classified_features_list
    }

    return classified_features
###Fin libreria concurrnt
#Fin funciones usadas para clasificar elevaciones paralelizando


###--->Función que clasifica elevaciones por rango
def classify_elevation(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999):
    
    range_div = (max_height - min_height + 1) / num_ranges

    
    classified_features = {
        "type": "FeatureCollection",
        "features": []
    }

    
    for feature in features_collection['features']:
        
        height = feature['properties']['heightBase']
        
        
        color_range = max(
            min(
                math.floor((height - min_height) / range_div),
                num_ranges
            ), -1
        )
        
       
        if color_range == -1:
            height_base = no_data_value  # Valor especial para fuera de rango
        else:
            height_base = math.ceil(color_range * range_div + min_height)

       
        classified_feature = {
            "type": "Feature",
            "geometry": feature['geometry'],
            "properties": {
                "heightBase": height_base,
                "colorRange": color_range
            }
        }

        
        classified_features["features"].append(classified_feature)

    return classified_features
###--->Fin Función que clasifica elevaciones por rango

####--->Función que agrupa datos paralelizando
# Función para procesar la unión de polígonos
def procesar_union(entrada):
    altura, poligonos = entrada
    union_poligonos = unary_union(poligonos)

    
    if isinstance(union_poligonos, Polygon):
        union_poligonos = [union_poligonos]
    elif isinstance(union_poligonos, MultiPolygon):
        union_poligonos = union_poligonos.geoms

    
    nuevas_features = []
    for poligono in union_poligonos:
        nuevas_features.append({
            "type": "Feature",
            "geometry": mapping(poligono),
            "properties": {"heightBase": altura}
        })

    return nuevas_features
####--->Fin Función que agrupa datos paralelizando

####--->Función paralelizada para agrupar tilas por altura
def agrupar_tilas_por_altura_paralelo(datos, num_procesos=12, chunk_size=5):
    #print(datos)
    print(" ")
    agrupaciones = {}
    print(len(agrupaciones))

    cantidad_features = len(datos["features"])
    print(f"La cantidad de features es: {cantidad_features}")
    
    for feature in datos["features"]:
        altura = feature["properties"]["heightBase"]
        geometry = json.loads(feature["geometry"])  # Convertir de string a dict
        poligono = shape(geometry)

        if altura not in agrupaciones:
            agrupaciones[altura] = []
        
        agrupaciones[altura].append(poligono)
    
    print(" ")
    print("agrupaciones",len(agrupaciones))
    print(" ")
    #print("agrupaciones",agrupaciones)
    entradas = [(altura, poligonos) for altura, poligonos in agrupaciones.items()]
    
   
    with Pool(num_procesos) as p:
        
        resultados = p.imap_unordered(procesar_union, entradas, chunksize=chunk_size)
        
        
        nuevas_features = [item for sublist in resultados for item in sublist]

   
    datos_agrupados = {
        "type": "FeatureCollection",
        "features": nuevas_features
    }

    return datos_agrupados
####--->Fin Función paralelizada para agrupar tilas por altura

####--->Función respaldo paralelizada para agrupar tilas por altura
def agrupar_tilas_por_altura_paralelo_respaldo(datos, num_procesos=12, chunk_size=5):
    agrupaciones = {}
    
    for feature in datos["features"]:
        altura = feature["properties"]["heightBase"]
        geometry = json.loads(feature["geometry"])  # Convertir de string a dict
        poligono = shape(geometry)

        if altura not in agrupaciones:
            agrupaciones[altura] = []
        
        agrupaciones[altura].append(poligono)
    
    
    entradas = [(altura, poligonos) for altura, poligonos in agrupaciones.items()]
    
   
    with Pool(num_procesos) as p:
        
        resultados = p.imap_unordered(procesar_union, entradas, chunksize=chunk_size)
        
        
        nuevas_features = [item for sublist in resultados for item in sublist]

   
    datos_agrupados = {
        "type": "FeatureCollection",
        "features": nuevas_features
    }

    return datos_agrupados
####--->Fin respaldo Función paralelizada para agrupar tilas por altura

####--->Función de agrupacion de datos
def agrupar_tilas_por_altura(datos):
    agrupaciones = {}
    
    for feature in datos["features"]:
        altura = feature["properties"]["heightBase"]
        geometry = json.loads(feature["geometry"])  # Convertir de string a dict
        poligono = shape(geometry)

        if altura not in agrupaciones:
            agrupaciones[altura] = []
        
        agrupaciones[altura].append(poligono)

    nuevas_features = []

    for altura, poligonos in agrupaciones.items():
        union_poligonos = unary_union(poligonos)

        if isinstance(union_poligonos, Polygon):
            union_poligonos = [union_poligonos]
        elif isinstance(union_poligonos, MultiPolygon):
            union_poligonos = union_poligonos.geoms

        for poligono in union_poligonos:
            nuevas_features.append({
                "type": "Feature",
                "geometry": mapping(poligono),
                "properties": {"heightBase": altura}
            })

    datos_agrupados = {
        "type": "FeatureCollection",
        "features": nuevas_features
    }

    return datos_agrupados
####--->Fin Función de agrupacion de datos

# Función para procesar los datos de elevación para un polígono y retornar un objeto JSON
def polygon_coloring_elevation_consulta_7(geometry, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("-------------Consulta 7: área grande > 900 km2 (sin adyacencia)")
    
    #Poligono de prueba para un area grande-->mas de 900km2
    #polygon = 'POLYGON((-3.41314 40.4762, -3.289893 40.4762, -3.289893 40.91916, -3.41314 40.91916, -3.41314 40.4762))'
    #print("polygon", polygon)

    #proceso geometry que es el poligono entrante
    #print("polygon", geometry)
    polygon = f"{geometry}"
    #print("polygon", polygon)


    # Obtener la sesión de la base de datos
    session = db.get_session()

    # Ejecutar la consulta con el polígono como parámetro
    try:
        inicio = time.perf_counter()
        result = session.execute(CONSULTA_7, {"polygon": polygon})
        fin = time.perf_counter()
        print(f"Tiempo de ejecución query a BBDD: {fin - inicio:.6f} segundos")

        # Obtener un solo resultado (dado que estamos esperando un único GeoJSON y estadísticas)
        row = result.fetchone()

        if row:
            # Desempaquetar los resultados: features_collection, min_height, max_height, avg_height
            features_collection, min_height, max_height, avg_height = row
            #print("features_collection",features_collection)
           #print(" ")
            #features_collection = row
            #print("features_collection",features_collection)

            # Imprimir o retornar los resultados
            #print("GeoJSON Features Collection:", features_collection)

            # with open("salida_sin_agrupar_crudos_area_200km2.json", "w") as archivo:
            #     json.dump(features_collection, archivo, indent=4)  

            # Función que no claifica --> no_classify_elevation
            # inicio_no_clasifica_elevaciones = time.perf_counter()
            # features_collection=no_classify_elevation(features_collection)
            # fin_no_clasifica_elevaciones = time.perf_counter()
            # print(f"Tiempo de ejecución_no_clasifica_elevaciones: {fin_no_clasifica_elevaciones - inicio_no_clasifica_elevaciones:.6f} segundos")
            

            # # Usar la versión sin paralelizar de la función para agrupar las tilas por altura
            # inicio_agrupacion = time.perf_counter()
            # features_collection=agrupar_tilas_por_altura(features_collection)
            # fin_agrupacion = time.perf_counter()
            # print(f"Tiempo de ejecución_agrupacion sin paralelizar: {fin_agrupacion - inicio_agrupacion:.6f} segundos")

            ##1
            # clasificación de elevaciones sin paralelizar
            inicio_clasifica_elevaciones = time.perf_counter()
            features_collection=classify_elevation(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999)
            fin_clasifica_elevaciones = time.perf_counter()
            print(f"Tiempo de ejecución_clasifica_elevaciones----: {fin_clasifica_elevaciones - inicio_clasifica_elevaciones:.6f} segundos")
            
            # clasificación de elevaciones paralelizando
            # inicio_clasifica_elevaciones_paralelizando = time.perf_counter()
            # #multiprocessing
            # features_collection=classify_elevation_parallel_multiprocessing(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999, num_processes=4, chunk_size=4)
            # #concurrent
            # #features_collection=classify_elevation_parallel_consurrent(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999, num_processes=8)
            # fin_clasifica_elevaciones_paralelizando = time.perf_counter()
            # print(f"Tiempo de ejecución_clasifica_elevaciones_paralelizando----: {fin_clasifica_elevaciones_paralelizando - inicio_clasifica_elevaciones_paralelizando:.6f} segundos")
            
            
            
            
            ##Fin 1

            ##2
            # Usar la versión paralelizada de la función para agrupar las tilas por altura
            inicio_agrupacion = time.perf_counter()
            features_collection = agrupar_tilas_por_altura_paralelo(features_collection, num_procesos=4, chunk_size=5)
            fin_agrupacion = time.perf_counter()
            print(f"Tiempo de ejecución_agrupacion paralelizando: {fin_agrupacion - inicio_agrupacion:.6f} segundos")
            ##Fin 2

            





            # print("Min Height:", min_height)
            # print("Max Height:", max_height)
            # print("Avg Height:", avg_height)
        else:
            print("No se devolvieron resultados.")

    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")

    # Retornar el objeto en formato JSON
    return features_collection, [min_height, max_height], avg_height




    ###--------Consulta 7 con EXPLAIN ANALIZE
def polygon_coloring_elevation_consulta_7_con_explain_analize(geometry, dataset):
    session = db.get_session()
    polygon = f"{geometry}"
    print("Polygon:", polygon)

    # Ejecutar EXPLAIN ANALYZE primero
    try:
        explain_result = session.execute(text("EXPLAIN ANALYZE " + CONSULTA_7.text), {"polygon": polygon})
        for row in explain_result:
            print(row[0])  # Imprime cada línea del plan de ejecución
    except Exception as e:
        print(f"Error en EXPLAIN ANALYZE: {e}")

    # Ejecutar la consulta real para obtener los resultados
    try:
        inicio = time.perf_counter()
        result = session.execute(CONSULTA_7, {"polygon": polygon})
        fin = time.perf_counter()
        print(f"Tiempo de ejecución con explain analize: {fin - inicio:.6f} segundos")

        # Obtener los resultados reales
        row = result.fetchone()

        if row:
            # Desempaquetar los resultados
            features_collection, min_height, max_height, avg_height = row

            # Guardar los resultados en un archivo JSON
            with open("salida_sin_agrupar_area_20_tilas.json", "w") as archivo:
                json.dump(features_collection, archivo, indent=4)
            print(f"Min Height: {min_height}, Max Height: {max_height}, Avg Height: {avg_height}")
        else:
            print("No se devolvieron resultados.")
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")

    # Retornar los resultados
    return features_collection, [min_height, max_height], avg_height
#-------------Fin Consulta 7-Con EXPLAIN ANALIZE: area grande > 900 km2 (sin adyacencia)

####Fin Nuevo analisis 22oct2024


##Original code for the polygon_elevation function
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

    Model = _getModel(dataset)
    
    if geometry.geom_type == 'Polygon':
        query_geom = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326) \
                            .label('geom')) \
                            .subquery().alias('pGeom')

        result_pixels = db.get_session() \
                            .query(func.DISTINCT(func.ST_PixelAsCentroids(
                                func.ST_Clip(Model.rast, query_geom.c.geom, NO_DATA_VALUE),
                                1, False))) \
                            .select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))) \
                            .all()
        
        point_col, height_col = format_PixelAsGeoms(result_pixels)

        raster_points3d = db.get_session() \
                            .query(func.ST_SetSRID(func.ST_MakePoint(ST_X(point_col),
                                                                     ST_Y(point_col),
                                                                     height_col),
                                              4326).label('geom')) \
                            .order_by(ST_X(point_col), ST_Y(point_col)) \
                            .subquery().alias('raster3d')

        query_points3d = db.get_session() \
                            .query(raster_points3d.c.geom) \
                            .select_from(raster_points3d) \
                            .join(query_geom, func.ST_Within(raster_points3d.c.geom, query_geom.c.geom)) \
                            .subquery().alias('points3d')

        if format_out == 'geojson':
            # Return GeoJSON directly in PostGIS
            query_final = db.get_session() \
                              .query(func.ST_AsGeoJson(func.ST_Collect(query_points3d.c.geom)))
            
        else:
            # Else return the WKT of the geometry
            query_final = db.get_session() \
                              .query(func.ST_AsText(func.ST_MakeLine(query_points3d.c.geom)))
    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))
    
    result_geom = query_final.scalar()

    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_geom
##Fin-Original code for the polygon_elevation function


##Start-Modified AAOR code for polygon_elevation function
def polygon_elevation_sql_simplificada_2_smt(geometry, format_out, dataset):
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
    
    if geometry.geom_type == 'Polygon':

        session = db.get_session()
        
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
            WHERE ST_Covers(pg_geom.polygon, pg.pixel_geom)
            ORDER BY ST_X(pg.pixel_geom), ST_Y(pg.pixel_geom);
        """
        #ORDER BY ST_X(pg.pixel_geom), ST_Y(pg.pixel_geom);-->measuring time with and without order by

        result_points = session.execute(text(consulta_sql), {"wkt_poligono": geometry.wkt}).fetchall()
            
    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))

    
    # Behaviour when all vertices are out of bounds
    if result_points == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
    print("polygon_elevation_sql_simplificada_2_smt--", result_points)
        
    return result_points

##End-Modified AAOR code for polygon_elevation function



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
