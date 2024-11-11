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
    print(" ")

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

    print("AreaRangesElevation-originallll - cmt")
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

        ####Codigo de verificacion 1

        # # Lista para almacenar los valores extraídos
        # extracted_values = []

        # # Extraer valores 3 y 4
        # for pixel in result_pixels:
        #     # Eliminar los paréntesis y dividir por comas
        #     parts = pixel[0].strip('()').split(',')
            
        #     # Extraer el valor 3 y 4 (índices 2 y 3)
        #     val3 = int(parts[2])
        #     val4 = int(parts[3])
            
        #     # Añadir la tupla (val3, val4) a la lista
        #     extracted_values.append((val3, val4))

        # # Verificar si hay repetidos usando un conjunto
        # unique_values = set(extracted_values)

        # # Calcular cuántos son repetidos
        # repeated_count = len(extracted_values) - len(unique_values)
        

        # if repeated_count > 0:
        #     print(f"Existen valores repetidos: {repeated_count} repetidos.")
        #     print(" ")
        #     print("extracted_values",len(extracted_values))
        #     print(" ")
        #     print("unique_values",len(unique_values))
        #     print(" ")
        # else:
        #     print("No hay valores repetidos.")

        ####Codigo de verificacion 2

        #Lista para almacenar las tuplas (val1, val3, val4)
        # extracted_values = []

        # # Extraer los valores 1, 3 y 4
        # for pixel in result_pixels:
        #     # Eliminar los paréntesis y dividir por comas
        #     parts = pixel[0].strip('()').split(',')
            
        #     # Extraer el valor 1, 3 y 4 (índices 0, 2 y 3)
        #     val1 = parts[0]    # Valor en la posición 1
        #     val3 = int(parts[2])  # Valor en la posición 3
        #     val4 = int(parts[3])  # Valor en la posición 4

        #     print("val1",val1)
        #     print("val3",val3)
        #     print("val4",val4)

            
        #     # Añadir la tupla (val1, val3, val4) a la lista
        #     extracted_values.append((val1, val3, val4))

        # # Usar un diccionario para contar las repeticiones por coordenadas (val3, val4)
        # repetitions = {}

        # for val1, val3, val4 in extracted_values:
        #     # Crear clave con las coordenadas (val3, val4)
        #     key = (val3, val4)
            
        #     # Si la clave ya existe en el diccionario
        #     if key in repetitions:
        #         # Añadir el valor 1 asociado a esas coordenadas
        #         repetitions[key].add(val1)
        #     else:
        #         # Crear un nuevo conjunto con el valor 1
        #         repetitions[key] = {val1}

        # # Comprobar si hay coordenadas (val3, val4) con más de un valor diferente en la posición 1
        # repeated_tuples = []
        # for key, val1_set in repetitions.items():
        #     if len(val1_set) > 1:
        #         # Añadir la coordenada repetida a la lista de tuplas repetidas
        #         repeated_tuples.append(key)

        # # Resultados
        # if repeated_tuples:
        #     print(f"Existen {len(repeated_tuples)} coordenadas (val3, val4) con valores diferentes en la posición 1.")
        #     print(f"Coordenadas repetidas: {repeated_tuples}")
        # else:
        #     print("No hay coordenadas (val3, val4) con valores diferentes en la posición 1.")



        #############
        # # Lista para almacenar las tuplas (val1, val3, val4)
        # extracted_values = []

        # # Extraer los valores 1, 3 y 4
        # for pixel in result_pixels:
        #     # Eliminar los paréntesis y dividir por comas
        #     parts = pixel[0].strip('()').split(',')
            
        #     # Extraer el valor 1, 3 y 4 (índices 0, 2 y 3)
        #     val1 = parts[0].strip()    # Valor en la posición 1
        #     val3 = int(parts[2].strip())  # Valor en la posición 3
        #     val4 = int(parts[3].strip())  # Valor en la posición 4

        #     # Imprimir los valores extraídos
        #     print("val1:", val1)
        #     print("val3:", val3)
        #     print("val4:", val4)

        #     # Añadir la tupla (val1, val3, val4) a la lista
        #     extracted_values.append((val1, val3, val4))

        # # Usar un diccionario para contar las repeticiones por coordenadas (val3, val4)
        # repetitions = {}

        # for val1, val3, val4 in extracted_values:
        #     # Crear clave con las coordenadas (val3, val4)
        #     key = (val3, val4)
            
        #     # Si la clave ya existe en el diccionario
        #     if key in repetitions:
        #         # Añadir el valor 1 asociado a esas coordenadas
        #         repetitions[key].add(val1)
        #     else:
        #         # Crear un nuevo conjunto con el valor 1
        #         repetitions[key] = {val1}

        # # Comprobar si hay coordenadas (val3, val4) con más de un valor diferente en la posición 1
        # repeated_tuples = []
        # for key, val1_set in repetitions.items():
        #     if len(val1_set) > 1:
        #         # Añadir la coordenada repetida a la lista de tuplas repetidas
        #         repeated_tuples.append(key)

        # # Resultados
        # if repeated_tuples:
        #     print(f"Existen {len(repeated_tuples)} coordenadas (val3, val4) con valores diferentes en la posición 1.")
        #     print(f"Coordenadas repetidas: {repeated_tuples}")
        # else:
        #     print("No hay coordenadas (val3, val4) con valores diferentes en la posición 1.")



        ####Codigo de extracción 3-->valida versión

        # # Inicializamos las listas
        # extracted_values = []
        # new_result_pixels = []

        # # Diccionario para rastrear las coordenadas (val3, val4) ya vistas
        # seen_coordinates = set()

        # # Recorrer los píxeles originales para extraer valores y filtrar duplicados
        # for pixel in result_pixels:
        #     # Eliminar los paréntesis y dividir por comas
        #     parts = pixel[0].strip('()').split(',')
            
        #     # Extraer los valores 1, 3 y 4 (índices 1, 2 y 3)
        #     val1 = parts[0]    # Valor en la posición 1 (WKT text)
        #     val2 = parts[1]  # Valor en la posición 3 (número entero)
        #     # Verificar si el valor de altura es igual a -32768
        #     if val2 == -32768:
        #         continue  # Saltar este pixel y continuar con el siguiente
        #     val3 = int(parts[2])  # Valor en la posición 3 (número entero)
        #     val4 = int(parts[3])  # Valor en la posición 4 (número entero)

        #     print("val1",val1)
        #     print("val3",val3)
        #     print("val4",val4)
            
        #     # Crear una clave con las coordenadas (val3, val4)
        #     coordinates = (val3, val4)
            
        #     # Si las coordenadas no se han visto antes, añadirlas a la lista final y al conjunto
        #     if coordinates not in seen_coordinates:
        #         # Añadir las coordenadas al conjunto de "vistas"
        #         seen_coordinates.add(coordinates)
                
        #         # Formatear la cadena de forma similar al formato original y añadir a new_result_pixels
        #         formatted_pixel = (f"({parts[0]},{val2},{val3},{val4})",)
        #         print("formatted_pixel------ojo",formatted_pixel)
        #         new_result_pixels.append(formatted_pixel)

        # # Crear la salida final en el formato deseado
        # resultado_final = [formatted_pixel for formatted_pixel in new_result_pixels]

        # # Imprimir el resultado final
        # print(resultado_final)

        #####Codigo de extracción 3-->valida versión y corregida

        # Inicializamos las listas
        extracted_values = []
        new_result_pixels = []

        # Diccionario para rastrear las coordenadas (val3, val4) ya vistas
        seen_coordinates = set()

        # Recorrer los píxeles originales para extraer valores y filtrar duplicados
        for pixel in result_pixels:
            # Eliminar los paréntesis y dividir por comas
            parts = pixel[0].strip('()').split(',')
            
            # Extraer los valores 1, 2, 3 y 4
            val1 = parts[0]  # Valor en la posición 1 (WKT text)
            val2 = int(parts[1].strip())  # Convertir a entero el valor en la posición 2 (número entero)
            
            # Verificar si el valor de altura es igual a -32768
            if val2 == -32768:
                continue  # Saltar este pixel y continuar con el siguiente
                
            val3 = int(parts[2].strip())  # Valor en la posición 3 (número entero)
            val4 = int(parts[3].strip())  # Valor en la posición 4 (número entero)

            # print("val1:", val1)
            # print("val3:", val3)
            # print("val4:", val4)
            
            # Crear una clave con las coordenadas (val3, val4)
            coordinates = (val3, val4)
            
            # Si las coordenadas no se han visto antes, añadirlas a la lista final y al conjunto
            if coordinates not in seen_coordinates:
                # Añadir las coordenadas al conjunto de "vistas"
                seen_coordinates.add(coordinates)
                
                # Formatear la cadena de forma similar al formato original y añadir a new_result_pixels
                formatted_pixel = (f"({val1},{val2},{val3},{val4})",)  # Asegúrate de usar el formato correcto
                #print("formatted_pixel------ojo:", formatted_pixel)
                new_result_pixels.append(formatted_pixel)

        # Crear la salida final en el formato deseado
        resultado_final = [formatted_pixel for formatted_pixel in new_result_pixels]

        # Imprimir el resultado final
        #print(resultado_final)

        ####Codigo de extracción 4-->valida versión - solo elimino los valores -32768

        # Inicializamos las listas
        # inicio_extraccion_4 = time.perf_counter()#AAOR 
        # extracted_values = []
        # new_result_pixels = []

        # # Diccionario para rastrear las coordenadas (val3, val4) ya vistas
        # seen_coordinates = set()

        # # Recorrer los píxeles originales para extraer valores y filtrar duplicados
        # for pixel in result_pixels:
        #     # Eliminar los paréntesis y dividir por comas
        #     parts = pixel[0].strip('()').split(',')
            
        #     # Extraer los valores 1, 2, 3 y 4
        #     val1 = parts[0]  # Valor en la posición 1 (WKT text)
        #     val2 = int(parts[1].strip())  # Convertir a entero el valor en la posición 2 (número entero)
            
        #     # Verificar si el valor de altura es igual a -32768
        #     if val2 == -32768:
        #         continue  # Saltar este pixel y continuar con el siguiente
                
        #     val3 = int(parts[2].strip())  # Valor en la posición 3 (número entero)
        #     val4 = int(parts[3].strip())  # Valor en la posición 4 (número entero)

        #     # print("val1:", val1)
        #     # print("val3:", val3)
        #     # print("val4:", val4)

        #     # Formatear la cadena de forma similar al formato original y añadir a new_result_pixels
        #     formatted_pixel = (f"({val1},{val2},{val3},{val4})",)  # Asegúrate de usar el formato correcto
        #     #print("formatted_pixel------ojo:", formatted_pixel)
        #     new_result_pixels.append(formatted_pixel)
            
        # # Crear la salida final en el formato deseado
        # resultado_final = [formatted_pixel for formatted_pixel in new_result_pixels]

        # fin_extraccion_4 = time.perf_counter()#AAOR
        # tiempo_transcurrido_extraccion_4 = fin_extraccion_4 - inicio_extraccion_4#AAOR
        # print(f"Tiempo de ejecución_extraccion_4: {tiempo_transcurrido_extraccion_4:.6f} segundos")#AAOR
        # print(" ")

        # Imprimir el resultado final
        #print(resultado_final)

        ######################################ooooooooooooooooo

        # # Comprobar si hay píxeles repetidos
        # for pixel in result_pixels:
        #     # Extraer la coordenada (valor 3 y valor 4)
        #     coord = (pixel[2], pixel[3])
            
        #     # Comprobar si ya existe en el conjunto
        #     if coord in pixel_coords:
        #         print(f"Píxel repetido encontrado: {coord}")
        #     else:
        #         pixel_coords.add(coord)

        
        inicio_consulta_queried_3 = time.perf_counter()#AAOR 
        polygon_col, height_col = format_PixelAsGeoms(resultado_final)
        #polygon_col, height_col = format_PixelAsGeoms(result_pixels)
        fin_consulta_queried_3 = time.perf_counter()#AAOR
        tiempo_transcurrido_consulta_queried_3 = fin_consulta_queried_3 - inicio_consulta_queried_3#AAOR
        print(f"Tiempo de ejecución_consulta_queried_3: {tiempo_transcurrido_consulta_queried_3:.6f} segundos")#AAOR
        #print("polygon_col",polygon_col)
        print(" ")
        #print("height_col",height_col)
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
            #print("query_features",query_features)
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


###Start-code based on the Hanli code for polygon_coloring_elevation function
# Consulta SQL optimizada
ELEVATION_UNION_FORMAT_SQL = text(
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
###New SQL

###New SQL-1
ELEVATION_UNION_FORMAT_SQL_1 = text(
    """
WITH query_geom AS (
    SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
),
filtered_data AS (
    SELECT ST_Clip(rast, 1, query_geom.geom, true) AS clipped_rast
    FROM oes_cgiar
    JOIN query_geom ON ST_Intersects(rast, query_geom.geom)
),
elevation_data AS (
    SELECT
        (dp).geom AS geometry,
        (dp).val AS height
    FROM filtered_data,
    LATERAL ST_DumpAsPolygons(clipped_rast) AS dp
    WHERE (dp).val > 0  -- Filtrar las alturas mayores a cero
)
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(
        json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geometry)::json,
            'properties', json_build_object('heightBase', height)
        )
    )
) AS geojson
FROM elevation_data;

"""
)


###Fin- SQL-1

###Inicia-SQL-2
ELEVATION_UNION_FORMAT_SQL_2 = text(
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


###Fin-SQL-2

def polygon_coloring_elevation_based_hanli(geom, dataset):
    """Procesa los datos de elevación para una geometría de polígono."""
    print("AreaRangesElevation-based on the Hanli-1111")
    # Extraer las coordenadas del polígono
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    # print(" ")
    print("SQL")
    geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL, {"polygon": polygon}).fetchone()

    # print(" ")
    # print("SQL_1")
    # geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_1, {"polygon": polygon}).fetchone()
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_1, {"polygon": polygon}).fetchone()
    

    print(" ")
    print("SQL_2")
    #geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_2, {"polygon": polygon}).fetchone()
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_1, {"polygon": polygon}).fetchone()

    print(" ")
    print(" ",geojson)
    print(" ")
    print("min_height",min_height)
    # print(" ")
    print("max_height",max_height)
    print(" ")
    print("avg_height",avg_height)
    #print("SQL_3")
    #geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_3, {"polygon": polygon}).fetchone()
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_3, {"polygon": polygon}).fetchone()
    
    #print("geojson[0]",geojson[0])

    # collection_queried = geojson[0]
    # result = []
    # for feature in collection_queried['features']:
    #     heightBase = int(feature['properties']['heightBase'])
    #     if feature['geometry']['type'] == 'Polygon':
    #         result.append(defs.UnitedArea(
    #             baseElevation=heightBase,
    #             area=self._create_proto_geo_polygon(feature['geometry']['coordinates']),
    #         ))
    #     else:
    #         for polygon in feature['geometry']['coordinates']:
    #             result.append(defs.UnitedArea(
    #                 baseElevation=heightBase,
    #                 area=self._create_proto_geo_polygon(polygon),
    #             ))

    # if geojson:
    #     geojson = geojson[0]  # Accedemos al geojson
    #     if isinstance(geojson, str):
    #         geojson = json.loads(geojson)
        
    #     # Acceder a las features
    #     features = geojson.get('features', [])
    #     for feature in features:
    #         geometry = feature.get('geometry', {})
    #         properties = feature.get('properties', {})
    #         height_base = properties.get('heightBase')
    #         print(f"Altura Base+++++: {height_base}")
    #print("aquiiiiiiiiiiiiii ", result)
    # print("geojson[1]",geojson[1])
    # # print(" ")
    # print("geojson[2]",geojson[2])
    # # print(" ")
    # print("geojson[3]",geojson[3])




    # print("SQL")
    # geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL, {"polygon": polygon}).fetchone()

    #print("SQL_1")
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_1, {"polygon": polygon}).fetchone()
    
    #print("SQL_2")
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_2, {"polygon": polygon}).fetchone()

    #print("SQL_3")
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_SQL_3, {"polygon": polygon}).fetchone()

    fin = time.perf_counter()

    #print("tipo-1", type(geojson))
    #print("geojson_desde_SQL_directo-1", geojson)

    # Desempaquetar resultados y tiempo de ejecución
    #inicio_carga = time.perf_counter()
    #geojson, min_height, max_height, avg_height = result
    #fin_carga = time.perf_counter()
    #print(f"Tiempo_carga: {fin_carga - inicio_carga:.6f} segundos")
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")
    print(" ")

    # Imprimir y devolver resultados
    #print(f"GeoJSON: {geojson}")
    #print(f"Altura mínima: {min_height}, Altura máxima: {max_height}, Altura promedio: {avg_height}")

    return geojson, [min_height, max_height], avg_height
    #return geojson

###End-code based on the Hanli code for polygon_coloring_elevation function

###Hanli-1

#####---->    (2)   ######

# Formato de la consulta SQL - h1
ELEVATION_UNION_FORMAT_h1 = text(
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
            'geometry', ST_AsGeojson(geometry)::jsonb,
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


# Función para procesar los datos de elevación para un polígono y retornar un objeto JSON
def polygon_coloring_elevation_based_hanli_1(geom, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("Procesando elevaciones basadas en el área de Hanli_1")
    
    # Extraer las coordenadas del polígono en formato WKT (Well-Known Text)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_h1, {"polygon": polygon}).fetchone()
    fin = time.perf_counter()

    print("min_height",min_height)
    print("max_height",max_height)
    print("avg_height",avg_height)

    # Desempaquetar resultados
    #geojson, min_height, max_height, avg_height = result
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")
    
    # Construir el objeto de respuesta JSON que incluye las estadísticas
    # response = {
    #     "geojson": geojson,
    #     "statistics": {
    #         "min_height": min_height,
    #         "max_height": max_height,
    #         "avg_height": avg_height
    #     }
    # }

    # Retornar el objeto en formato JSON
    return geojson, [min_height, max_height], avg_height

    #####---->    (3)   ######

    # Formato de la consulta SQL - _sin_min_max_avg
ELEVATION_UNION_FORMAT_h1_sin_min_max_avg = text(
    """

WITH query_geom AS (
    SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
),
clipped_rast AS (
    -- Recortar el raster a la geometría de entrada
    SELECT ST_Clip(oes_cgiar.rast, 1, query_geom.geom, true) AS rast
    FROM oes_cgiar
    JOIN query_geom ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
),
polygons AS (
    -- Convertir el raster recortado en polígonos
    SELECT (ST_DumpAsPolygons(rast)).*
    FROM clipped_rast
),
filtered_polygons AS (
    -- Filtrar los polígonos con valor mayor a cero
    SELECT *
    FROM polygons
    WHERE val > 0
)
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(
        jsonb_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geom)::jsonb,
            'properties', json_build_object('height', val)
        )
    )
)
FROM filtered_polygons;


    """
)


    # Función para procesar los datos de elevación para un polígono y retornar un objeto JSON
def polygon_coloring_elevation_based_hanli_sin_min_max_avg(geom, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("Procesando elevaciones basadas en el área de Hanli_1 sin_min_max_avg")
    
    # Extraer las coordenadas del polígono en formato WKT (Well-Known Text)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_sin_min_max_avg, {"polygon": polygon}).fetchone()
    fin = time.perf_counter()
    #print("min_height",geojson)
    # print("min_height",min_height)
    # print("max_height",max_height)
    # print("avg_height",avg_height)

    # Desempaquetar resultados
    #geojson, min_height, max_height, avg_height = result
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")
    
    # Construir el objeto de respuesta JSON que incluye las estadísticas
    # response = {
    #     "geojson": geojson,
    #     "statistics": {
    #         "min_height": min_height,
    #         "max_height": max_height,
    #         "avg_height": avg_height
    #     }
    # }

    # Retornar el objeto en formato JSON
    return geojson

#####---->    (4)   ######

    # Formato de la consulta SQL - optimizado_aaor

    # Función para procesar los datos de elevación para un polígono y retornar un objeto JSON
def polygon_coloring_elevation_based_hanli_optimizado_aaor(geom, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("Procesando elevaciones basadas en el área de Hanli_1 _optimizado_aaor")
    
    # Extraer las coordenadas del polígono en formato WKT (Well-Known Text)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    #geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_optimizado_aaor, {"polygon": polygon}).fetchone()
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
    geojson = session.execute(text(consulta_sql), {"wkt_poligono": geom.wkt}).fetchall()
    fin = time.perf_counter()
    print("min_height",geojson)
    # print("min_height",min_height)
    # print("max_height",max_height)
    # print("avg_height",avg_height)

    # Desempaquetar resultados
    #geojson, min_height, max_height, avg_height = result
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")
    
    # Construir el objeto de respuesta JSON que incluye las estadísticas
    # response = {
    #     "geojson": geojson,
    #     "statistics": {
    #         "min_height": min_height,
    #         "max_height": max_height,
    #         "avg_height": avg_height
    #     }
    # }

    # Retornar el objeto en formato JSON
    return geojson
###Fin-Hanli-1


#####---->    (5)   ###### h1_paralelizacion

# Formato de la consulta SQL - h1



ELEVATION_UNION_FORMAT_h1_paralelizacion = text(
    """
    WITH query_geom AS (
        SELECT ST_SetSRID(ST_MakePolygon(
            ST_GeomFromText(:polygon)
        ), 4326) AS geom
    )
    SELECT 
        val AS height, 
        ST_AsGeoJSON(geometry)::jsonb AS geometry
    FROM (
        SELECT DISTINCT val, 
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


# Función para procesar los datos de elevación para un polígono y retornar un objeto JSON
def polygon_coloring_elevation_based_hanli_1_paralelizacion(geom, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("Procesando elevaciones basadas en el área de Hanli_1_paralelizacion")
    
    # Extraer las coordenadas del polígono en formato WKT (Well-Known Text)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_paralelizacion, {"polygon": polygon}).fetchone()
    #geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_h1, {"polygon": polygon}).fetchone()[0]
    

    # Invocando la consulta
    result = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_paralelizacion, {"polygon": polygon}).fetchall()

    # Procesar resultados
    # for row in result:
    #     height = row.height
    #     geometry = row.geometry
    #     #print(f"Height: {height}, Geometry: {geometry}")

        
    fin = time.perf_counter()

    # print("min_height",min_height)
    # print("max_height",max_height)
    # print("avg_height",avg_height)

    # Desempaquetar resultados
    #geojson, min_height, max_height, avg_height = result
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")
    
    # Construir el objeto de respuesta JSON que incluye las estadísticas
    # response = {
    #     "geojson": geojson,
    #     "statistics": {
    #         "min_height": min_height,
    #         "max_height": max_height,
    #         "avg_height": avg_height
    #     }
    # }

    # Retornar el objeto en formato JSON
    return geojson, [min_height, max_height], avg_height


#
#####---->    (6)   ###### paralelización_1_2

# Formato de la consulta SQL - h1



ELEVATION_UNION_FORMAT_h1_paralelizacion_1 = text(
    """
WITH query_geom AS (
        SELECT ST_SetSRID(ST_MakePolygon(
            ST_GeomFromText(:polygon)
        ), 4326) AS geom
    )
    SELECT 
        ST_AsText(geometry) AS polygon,  -- Convertir el polígono a texto para mayor legibilidad
        height 
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
    ORDER BY height;  -- Ordenar por altura
    """
)


ELEVATION_UNION_FORMAT_h1_paralelizacion_2 = text(
    """   
WITH query_geom AS (
    SELECT ST_SetSRID(ST_MakePolygon(ST_GeomFromText(:polygon)), 4326) AS geom
),
clipped_raster AS (
    SELECT ST_Clip(oes_cgiar.rast, query_geom.geom, 0) AS rast
    FROM oes_cgiar, query_geom
    WHERE ST_Intersects(oes_cgiar.rast, query_geom.geom)  -- Filtra solo los rasters que intersectan
),
polygons AS (
    SELECT (ST_DumpAsPolygons(rast)).* 
    FROM clipped_raster
)
SELECT *
FROM polygons;  -- Limitar r
    """
)



# Función para procesar los datos de elevación para un polígono y retornar un objeto JSON --
def polygon_coloring_elevation_based_hanli_1_paralelizacion_1_2(geom, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("Procesando elevaciones basadas en el área de Hanli_1_paralelizacion_1_o_2")
    
    # Extraer las coordenadas del polígono en formato WKT (Well-Known Text)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_paralelizacion, {"polygon": polygon}).fetchone()
    #geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_h1, {"polygon": polygon}).fetchone()[0]
    

    # Invocando la consulta
    result = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_paralelizacion_1, {"polygon": polygon}).fetchall()
    result = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_paralelizacion_2, {"polygon": polygon}).fetchall()
    #print(result)

    # Procesar resultados
    # for row in result:
    #     height = row.height
    #     geometry = row.geometry
    #     #print(f"Height: {height}, Geometry: {geometry}")

        
    fin = time.perf_counter()

    # print("min_height",min_height)
    # print("max_height",max_height)
    # print("avg_height",avg_height)

    # Desempaquetar resultados
    #geojson, min_height, max_height, avg_height = result
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")
    
    # Construir el objeto de respuesta JSON que incluye las estadísticas
    # response = {
    #     "geojson": geojson,
    #     "statistics": {
    #         "min_height": min_height,
    #         "max_height": max_height,
    #         "avg_height": avg_height
    #     }
    # }

    # Retornar el objeto en formato JSON
    return geojson, [min_height, max_height], avg_height



#####---->    (7)   ###### prueba_paralelizacion

ELEVATION_UNION_FORMAT_prueba_paralelizacion = text(
    """
    WITH query_geom AS (
        SELECT ST_SetSRID(ST_MakePolygon(
            ST_GeomFromText(:polygon)
        ), 4326) AS geom
    )
    SELECT 
        val AS height, 
        ST_AsGeoJSON(geometry)::jsonb AS geometry
    FROM (
        SELECT DISTINCT val, 
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


# Función para procesar los datos de elevación para un polígono y retornar un objeto JSON
def polygon_coloring_elevation_prueba_paralelizacion(geom, dataset):
    """Procesa los datos de elevación para una geometría de polígono y devuelve un JSON."""
    print("polygon_coloring_elevation_prueba_paralelizacion_(7)")
    
    # Extraer las coordenadas del polígono en formato WKT (Well-Known Text)
    coordinates = ", ".join(f"{lon} {lat}" for lon, lat in geom.exterior.coords)
    polygon = f"LINESTRING({coordinates})"

    # Ejecutar la consulta SQL y medir el tiempo
    inicio = time.perf_counter()
    #geojson, min_height, max_height, avg_height = db.get_session().execute(ELEVATION_UNION_FORMAT_h1_paralelizacion, {"polygon": polygon}).fetchone()
    #geojson = db.get_session().execute(ELEVATION_UNION_FORMAT_h1, {"polygon": polygon}).fetchone()[0]
    

    # Invocando la consulta
    result = db.get_session().execute(ELEVATION_UNION_FORMAT_prueba_paralelizacion, {"polygon": polygon}).fetchall()
    
    
    # Procesar resultados
    # for row in result:
    #     height = row.height
    #     geometry = row.geometry
    #     #print(f"Height: {height}, Geometry: {geometry}")

        
    fin = time.perf_counter()

    # print("min_height",min_height)
    # print("max_height",max_height)
    # print("avg_height",avg_height)

    # Desempaquetar resultados
    #geojson, min_height, max_height, avg_height = result
    print(f"Tiempo de ejecución: {fin - inicio:.6f} segundos")
    
    # Construir el objeto de respuesta JSON que incluye las estadísticas
    # response = {
    #     "geojson": geojson,
    #     "statistics": {
    #         "min_height": min_height,
    #         "max_height": max_height,
    #         "avg_height": avg_height
    #     }
    # }

    # Retornar el objeto en formato JSON
    return geojson, [min_height, max_height], avg_height
#



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


###new
def polygon_coloring_elevation_new(geometry, dataset):
    """
    Optimized version to perform PostGIS query to enrich a polygon geometry.
    """
    print("polygon_coloring_elevation_new")
    Model = _getModel(dataset)
    num_ranges = 23

    if geometry.geom_type != 'Polygon':
        raise InvalidUsage(400, 4002, f"Needs to be a Polygon, not a {geometry.geom_type}!")

    # Convert the input geometry to the proper SRID and format
    session = db.get_session()
    query_geom = func.ST_SetSRID(func.ST_GeomFromText(geometry.wkt), 4326).label('geom')

    # Use ST_Clip to clip the raster with the input geometry
    clipped_raster_subquery = session.query(
        func.ST_Clip(Model.rast, query_geom, True).label('clipped_rast')
    ).filter(
        func.ST_Intersects(Model.rast, query_geom)
    ).subquery(name='clipped_raster')

    # Compute statistics directly on the clipped raster
    stats = session.query(
        func.ST_MinMax(clipped_raster_subquery.c.clipped_rast, 1, True)
    ).one()[0]

    if stats is None:
        raise InvalidUsage(400, 4002, 'The requested geometry does not contain any elevation data')

    min_height = stats['min']
    max_height = stats['max']
    avg_height = session.query(
        func.ST_Mean(clipped_raster_subquery.c.clipped_rast, 1, True)
    ).scalar()

    if min_height is None or max_height is None:
        raise InvalidUsage(400, 4002,
                           'The requested geometry does not contain any elevation data')

    range_div = (max_height - min_height + 1) / num_ranges

    # Use ST_DumpAsPolygons to get polygons with their values
    polygons_subquery = session.query(
        func.ST_DumpAsPolygons(clipped_raster_subquery.c.clipped_rast).label('geomval')
    ).subquery(name='polygons')

    # Extract geometry and value from geomval
    polygons_with_values = session.query(
        polygons_subquery.c.geomval['geom'].label('geometry'),
        polygons_subquery.c.geomval['val'].label('height')
    ).subquery(name='polygons_with_values')

    # Assign color ranges based on height
    ranged_set = session.query(
        polygons_with_values.c.geometry,
        func.GREATEST(
            func.LEAST(
                func.floor((polygons_with_values.c.height - min_height) / range_div),
                num_ranges
            ), -1
        ).label("colorRange")
    ).filter(polygons_with_values.c.height != NO_DATA_VALUE).subquery(name='ranged_set')

    # Build GeoJSON features
    query_features = session.query(
        func.jsonb_build_object(
            'type', 'Feature',
            'geometry', func.ST_AsGeoJSON(
                func.ST_Union(func.array_agg(
                    func.ST_SnapToGrid(ranged_set.c.geometry, 1e-12)
                ))
            ).cast(JSON),
            'properties', func.json_build_object(
                'heightBase', func.ceil(ranged_set.c.colorRange * range_div + min_height)
            )
        ).label('features')
    ).group_by(ranged_set.c.colorRange).subquery(name='query_features')

    # Build the final GeoJSON FeatureCollection
    query_final = session.query(
        func.jsonb_build_object(
            'type', 'FeatureCollection',
            'features', func.jsonb_agg(query_features.c.features)
        )
    )

    result_geom = query_final.scalar()

    if result_geom is None:
        raise InvalidUsage(404, 4002,
                           f'The requested geometry is outside the bounds of {dataset}')

    return result_geom, [min_height, max_height], avg_height

###fin new

###new 2
def polygon_coloring_elevation_new_2(geometry, dataset):
    print("polygon_coloring_elevation_new_2")
    Model = _getModel(dataset)
    num_ranges = 23

    if geometry.geom_type == 'Polygon':
        session = db.get_session()
        query_geom = session.query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326).label('geom')).subquery().alias('pGeom')

        result_pixels = session.query(
                            func.ST_PixelAsPolygons(func.ST_Clip(Model.rast, query_geom.c.geom, NO_DATA_VALUE), 1, False)
                        ).select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))).all()

        polygon_col, height_col = format_PixelAsGeoms(result_pixels)

        filtered_set = session.query(
                            polygon_col.label("geometry"),
                            height_col.label("height")
                        ).join(query_geom, func.ST_Within(func.ST_Centroid(polygon_col), query_geom.c.geom)).subquery()

        min_height, max_height, avg_height = session.query(
                            func.min(filtered_set.c.height),
                            func.max(filtered_set.c.height),
                            func.avg(filtered_set.c.height)
                        ).filter(filtered_set.c.height != NO_DATA_VALUE).one()

        if min_height is None or max_height is None:
            raise InvalidUsage(400, 4002, 'No elevation data')
        else:
            range_div = (max_height - min_height + 1) / num_ranges
            ranged_set = session.query(
                            filtered_set.c.geometry,
                            func.GREATEST(func.LEAST(func.floor((filtered_set.c.height - min_height) / range_div), num_ranges), -1).label("colorRange")
                        ).subquery()

            query_features = session.query(
                                func.jsonb_build_object(
                                    'type', 'Feature',
                                    'geometry', func.ST_AsGeoJson(
                                        func.ST_SimplifyPreserveTopology(func.ST_Union(func.array_agg(func.ST_ReducePrecision(ranged_set.c.geometry, 1e-12))), 1e-12)
                                    ).cast(JSON),
                                    'properties', func.json_build_object(
                                        'heightBase', case(
                                            (ranged_set.c.colorRange < 0, NO_DATA_VALUE),
                                            else_ = func.ceil(ranged_set.c.colorRange * range_div + min_height)
                                        ),
                                    )).label('features')
                            ).group_by(ranged_set.c.colorRange).subquery()

            query_final = session.query(func.jsonb_build_object(
                                'type', 'FeatureCollection',
                                'features', func.jsonb_agg(query_features.c.features)
                            )).scalar()

        if query_final is None:
            raise InvalidUsage(404, 4002, 'Outside bounds')

        return query_final, [min_height, max_height], avg_height
    else:
        raise InvalidUsage(400, 4002, f"Needs to be a Polygon, not {geometry.geom_type}")

###fin new 2

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
