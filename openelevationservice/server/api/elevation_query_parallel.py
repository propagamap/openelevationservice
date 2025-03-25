from sqlalchemy import text
import json
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
#from shapely.ops import unary_union
import math

#AAOR-test-start
from tool_test import measure_time
import time
import json
from collections import defaultdict
from shapely import from_geojson, unary_union
#AAOR-test-end


POLYGON_COLORING_ELEVATION_QUERY = text(
    """
    WITH query_geom AS (
    SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
),
polygons AS (
    SELECT *
    FROM (
        SELECT 
            (ST_PixelAsPolygons(
                ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
                1, False
            )).geom AS geometry, 
            (ST_PixelAsPolygons(
                ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
                1, False
            )).val AS height  
        FROM query_geom 
        JOIN oes_cgiar 
        ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
    ) AS subquery
    WHERE height != 0  
)
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

#new-code

@measure_time #AAOR-test
def group_and_union(features_collection, min_height, max_height, num_ranges):
    """
    Agrupa polígonos por altura, realiza uniones y devuelve features GeoJSON
    
    :param features_collection: GeoJSON FeatureCollection
    :return: Lista de features GeoJSON con polígonos unidos por altura
    """
    range_div = (max_height - min_height + 1) / num_ranges
    groupings = defaultdict(list)
    
    # Primera pasada: agrupar geometrías (manteniendo JSON)
    for feature in features_collection['features']:
        height = feature['properties']['heightBase']
        color_range = math.floor((height - min_height) / range_div)
        height_base = math.ceil(color_range * range_div + min_height)
        groupings[height_base].append(feature['geometry'])  # Guardamos el string GeoJSON
    
    # Segunda pasada: procesamiento y formato de salida
    new_features = []
    
    for height, geojson_strings in groupings.items():
        if not geojson_strings:
            continue
            
        if len(geojson_strings) == 1:
            # Caso simple: un solo polígono (sin necesidad de unión)
            polygon = from_geojson(geojson_strings[0])
        else:
            # Unión de múltiples polígonos
            geoms = [from_geojson(s) for s in geojson_strings]
            polygon = unary_union(geoms)
        
        # Manejo de MultiPolygon resultante de uniones
        if polygon.geom_type == 'MultiPolygon':
            for poly in polygon.geoms:
                new_features.append({
                    "type": "Feature",
                    "geometry": mapping(poly),
                    "properties": {"heightBase": height}
                })
        else:
            new_features.append({
                "type": "Feature",
                "geometry": mapping(polygon),
                "properties": {"heightBase": height}
            })
    
    return new_features
    
   
@measure_time #AAOR-test
#def group_tiles_by_height_without_parallel(data):
def group_tiles_by_height_without_parallel(
            features_collection,
            min_height,
            max_height,
            num_ranges
        ):
    """
    Groups tiles by elevation value and merges them in parallel for improved performance.

    :param data: Feature collection containing polygons with elevation values.
    :type data: dict

    :param num_processes: Number of parallel processes to use (default: 12).
    :type num_processes: int

    :param chunk_size: Number of elements per processing chunk (default: 5).
    :type chunk_size: int

    :returns: A feature collection with merged polygons grouped by elevation.
    :rtype: dict
    """
    
    #entries=group_tiles_by_height(data)
    entries=group_and_union(features_collection, min_height, max_height, num_ranges)
    
    #print(" ")
    #print("entries of group_and_union", entries)
    
    

    new_features = []
    
    new_features.extend(entries) 
    
    #print("new_features", new_features) 
  
   
    grouped_data = {
        "type": "FeatureCollection",
        "features": new_features
    }
    

    return grouped_data

#fin-nuevo enfoque
#end-new-code





#old code
# def classify_elevation_ordered(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999):
#     """
#     Categorizes elevation values into discrete ranges and assigns color bands.
#     Also ensures that the output features are sorted by elevation.

#     :param features_collection: GeoJSON feature collection containing elevation data.
#     :type features_collection: dict

#     :param min_height: Minimum elevation value in the dataset.
#     :type min_height: float

#     :param max_height: Maximum elevation value in the dataset.
#     :type max_height: float

#     :param num_ranges: Number of elevation ranges (default: 23).
#     :type num_ranges: int

#     :param no_data_value: Value assigned to pixels with missing data (default: -9999).
#     :type no_data_value: int

#     :returns: A feature collection with categorized elevation values, sorted by heightBase.
#     :rtype: dict
#     """
       
#     range_div = (max_height - min_height + 1) / num_ranges

#     buckets = [[] for _ in range(num_ranges + 1)]  

#     for feature in features_collection['features']:
#         height = feature['properties']['heightBase']

#         if height == no_data_value:
#             color_range = -1
#         else:
#             color_range = max(
#                 min(
#                     math.floor((height - min_height) / range_div),
#                     num_ranges - 1
#                 ), 0
#             )

#         if color_range == -1:
#             height_base = no_data_value
#         else:
#             height_base = math.ceil(color_range * range_div + min_height)

#         classified_feature = {
#             "type": "Feature",
#             "geometry": feature['geometry'],
#             "properties": {
#                 "heightBase": height_base,
#                 "colorRange": color_range
#             }
#         }

#         buckets[color_range + 1].append(classified_feature)  

#     classified_features = {
#         "type": "FeatureCollection",
#         "features": [feature for bucket in buckets for feature in bucket]
#     }

   
#     return classified_features


# def classify_elevation(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999):
#     """
#     Categorizes elevation values into discrete ranges and assigns color bands.

#     :param features_collection: GeoJSON feature collection containing elevation data.
#     :type features_collection: dict

#     :param min_height: Minimum elevation value in the dataset.
#     :type min_height: float

#     :param max_height: Maximum elevation value in the dataset.
#     :type max_height: float

#     :param num_ranges: Number of elevation ranges (default: 23).
#     :type num_ranges: int

#     :param no_data_value: Value assigned to pixels with missing data (default: -9999).
#     :type no_data_value: int

#     :returns: A feature collection with categorized elevation values.
#     :rtype: dict
#     """

    
#     range_div = (max_height - min_height + 1) / num_ranges

#     classified_features = {
#         "type": "FeatureCollection",
#         "features": []
#     }

#     for feature in features_collection['features']:
        
#         height = feature['properties']['heightBase']
        
        
#         color_range = max(
#             min(
#                 math.floor((height - min_height) / range_div),
#                 num_ranges
#             ), -1
#         )
          
#         if color_range == -1:
#             height_base = no_data_value 
#         else:
#             height_base = math.ceil(color_range * range_div + min_height)
 
#         classified_feature = {
#             "type": "Feature",
#             "geometry": feature['geometry'],
#             "properties": {
#                 "heightBase": height_base,
#                 "colorRange": color_range
#             }
#         }
   
#         classified_features["features"].append(classified_feature)
    
    
#     return classified_features


# def process_union(input_data):
#     """
#     Merges polygons that share the same elevation value into a unified geometry.

#     :param input_data: Tuple containing an elevation value and a list of polygon geometries.
#     :type input_data: tuple(float, list[Polygon])

#     :returns: A list of GeoJSON features with merged polygons.
#     :rtype: list[dict]
#     """
#     height, polygons = input_data
    
#     union_polygons = unary_union(polygons)

#     if isinstance(union_polygons, Polygon):
#         union_polygons = [union_polygons]
#     elif isinstance(union_polygons, MultiPolygon):
#         union_polygons = union_polygons.geoms

#     new_features = []
#     for polygon in union_polygons:
#         new_features.append({
#             "type": "Feature",
#             "geometry": mapping(polygon),
#             "properties": {"heightBase": height}
#         })

#     return new_features


# def group_tiles_by_height(data):
#     """
#     Groups tiles by elevation value.

#     :param data: Collection of features containing polygons with elevation values.
#     type data: dict

#     :returns: Grouped entries for Cython processing (as a list of tuples).
#     :rtype: list
#     """

#     groupings = {}
    
#     for feature in data["features"]:
#         height = feature["properties"]["heightBase"]
#         geometry = json.loads(feature["geometry"])  
#         polygon = shape(geometry)

#         if height not in groupings:
#             groupings[height] = []
        
#         groupings[height].append(polygon)
    
#     entries = [(height, polygons) for height, polygons in groupings.items()]

#     return entries


# def group_tiles_by_height_without_parallel(data):
#     """
#     Groups tiles by elevation value and merges them in parallel for improved performance.

#     :param data: Feature collection containing polygons with elevation values.
#     :type data: dict

#     :param num_processes: Number of parallel processes to use (default: 12).
#     :type num_processes: int

#     :param chunk_size: Number of elements per processing chunk (default: 5).
#     :type chunk_size: int

#     :returns: A feature collection with merged polygons grouped by elevation.
#     :rtype: dict
#     """
    
#     entries=group_tiles_by_height(data)

#     new_features = []
#     for entry in entries:
#         result = process_union(entry)  
#         new_features.extend(result)  
  
   
#     grouped_data = {
#         "type": "FeatureCollection",
#         "features": new_features
#     }
    

#     return grouped_data


