from sqlalchemy import text
import json
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.ops import unary_union
from multiprocessing import Pool
import math

#AAOR-test
import time
from collections import defaultdict




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



def classify_elevation_ordenada(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999):
    """
    Categorizes elevation values into discrete ranges and assigns color bands.
    Also ensures that the output features are sorted by elevation.

    :param features_collection: GeoJSON feature collection containing elevation data.
    :type features_collection: dict

    :param min_height: Minimum elevation value in the dataset.
    :type min_height: float

    :param max_height: Maximum elevation value in the dataset.
    :type max_height: float

    :param num_ranges: Number of elevation ranges (default: 23).
    :type num_ranges: int

    :param no_data_value: Value assigned to pixels with missing data (default: -9999).
    :type no_data_value: int

    :returns: A feature collection with categorized elevation values, sorted by heightBase.
    :rtype: dict
    """
    start_time = time.time()
    
    range_div = (max_height - min_height + 1) / num_ranges

    # Inicializar una lista de buckets para cada rango
    buckets = [[] for _ in range(num_ranges + 1)]  # +1 para el valor no_data

    for feature in features_collection['features']:
        height = feature['properties']['heightBase']

        # Calcular el rango de color
        if height == no_data_value:
            color_range = -1
        else:
            color_range = max(
                min(
                    math.floor((height - min_height) / range_div),
                    num_ranges - 1
                ), 0
            )

        # Calcular el valor base de altura para el rango
        if color_range == -1:
            height_base = no_data_value
        else:
            height_base = math.ceil(color_range * range_div + min_height)

        # Crear la feature clasificada
        classified_feature = {
            "type": "Feature",
            "geometry": feature['geometry'],
            "properties": {
                "heightBase": height_base,
                "colorRange": color_range
            }
        }

        # Asignar la feature al bucket correspondiente
        buckets[color_range + 1].append(classified_feature)  # +1 para evitar índices negativos

    # Concatenar los buckets en orden
    classified_features = {
        "type": "FeatureCollection",
        "features": [feature for bucket in buckets for feature in bucket]
    }

    # Finalizar medición de tiempo
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de classify_elevation_ordenada: {elapsed_time:.4f} segundos")

    json_data = json.dumps(classified_features, default=lambda obj: str(obj), indent=4)
    # print("data que sale a classify_elevation:")
    # print(json_data)
    # print(" ")

    with open('sale_classify_elevation_ordenada.txt', 'w') as f:
        f.write(json_data)

    return classified_features


def classify_elevation_optimizada(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999):
    """
    Categorizes elevation values into discrete ranges and assigns color bands.
    """
    
    start_time = time.time()

    # Calcular el tamaño de cada rango
    range_div = (max_height - min_height + 1) / num_ranges

    # Diccionario para agrupar features por heightBase
    height_groups = defaultdict(list)

    # Agrupar features por heightBase
    for feature in features_collection['features']:
        height = feature['properties']['heightBase']
        height_groups[height].append(feature)

    # Diccionario para almacenar el color_range de cada heightBase
    color_range_cache = {}

    # Lista para almacenar los features clasificados
    classified_features = {
        "type": "FeatureCollection",
        "features": []
    }

    # Calcular el color_range para cada heightBase único
    for height, features in height_groups.items():
        if height in color_range_cache:
            color_range = color_range_cache[height]
        else:
            color_range = max(
                min(
                    math.floor((height - min_height) / range_div),
                    num_ranges
                ), -1
            )
            color_range_cache[height] = color_range

        # Calcular height_base
        if color_range == -1:
            height_base = no_data_value
        else:
            height_base = math.ceil(color_range * range_div + min_height)

        # Asignar el color_range y height_base a cada feature del grupo
        for feature in features:
            classified_feature = {
                "type": "Feature",
                "geometry": feature['geometry'],
                "properties": {
                    "heightBase": height_base,
                    "colorRange": color_range
                }
            }
            classified_features["features"].append(classified_feature)

    # Finalizar medición de tiempo
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de classify_elevation_optimizada: {elapsed_time:.4f} segundos")

    json_data = json.dumps(classified_features, default=lambda obj: str(obj), indent=4)
    # print("data que sale a classify_elevation:")
    # print(json_data)
    # print(" ")

    with open('sale_classify_elevation_optimizada.txt', 'w') as f:
        f.write(json_data)

    return classified_features


def classify_elevation(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999):
    """
    Categorizes elevation values into discrete ranges and assigns color bands.

    :param features_collection: GeoJSON feature collection containing elevation data.
    :type features_collection: dict

    :param min_height: Minimum elevation value in the dataset.
    :type min_height: float

    :param max_height: Maximum elevation value in the dataset.
    :type max_height: float

    :param num_ranges: Number of elevation ranges (default: 23).
    :type num_ranges: int

    :param no_data_value: Value assigned to pixels with missing data (default: -9999).
    :type no_data_value: int

    :returns: A feature collection with categorized elevation values.
    :rtype: dict
    """

    json_data = json.dumps(features_collection, default=lambda obj: str(obj), indent=4)
    # print("data que llega a classify_elevation:")
    # print(json_data)
    # print(" ")

    with open('entra_classify_elevation.txt', 'w') as f:
        f.write(json_data)
    
    
    start_time = time.time()

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
            height_base = no_data_value 
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
    
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de classify_elevation: {elapsed_time:.4f} segundos")

    json_data = json.dumps(classified_features, default=lambda obj: str(obj), indent=4)
    # print("data que sale a classify_elevation:")
    # print(json_data)
    # print(" ")

    with open('sale_classify_elevation.txt', 'w') as f:
        f.write(json_data)
    
    return classified_features


def process_union(input_data):
    """
    Merges polygons that share the same elevation value into a unified geometry.

    :param input_data: Tuple containing an elevation value and a list of polygon geometries.
    :type input_data: tuple(float, list[Polygon])

    :returns: A list of GeoJSON features with merged polygons.
    :rtype: list[dict]
    """
    height, polygons = input_data
    
    union_polygons = unary_union(polygons)

    if isinstance(union_polygons, Polygon):
        union_polygons = [union_polygons]
    elif isinstance(union_polygons, MultiPolygon):
        union_polygons = union_polygons.geoms

    new_features = []
    for polygon in union_polygons:
        new_features.append({
            "type": "Feature",
            "geometry": mapping(polygon),
            "properties": {"heightBase": height}
        })

    return new_features


def group_tiles_by_height(data):
    """
    Groups tiles by elevation value.

    :param data: Collection of features containing polygons with elevation values.
    type data: dict

    :returns: Grouped entries for Cython processing (as a list of tuples).
    :rtype: list
    """

    # json_data = json.dumps(data, default=lambda obj: str(obj), indent=4)
    # print("data que llega a group_tiles_by_height:")
    # print(json_data)
    # print(" ")


    # group by height
    groupings = {}
    
    for feature in data["features"]:
        height = feature["properties"]["heightBase"]
        geometry = json.loads(feature["geometry"])  
        polygon = shape(geometry)

        if height not in groupings:
            groupings[height] = []
        
        groupings[height].append(polygon)
    
    # Convert groups into a list of tuples (height, list of polygons)
    entries = [(height, polygons) for height, polygons in groupings.items()]

    json_data = json.dumps(entries, default=lambda obj: str(obj), indent=4)
    #print("Datos entries enviados a process_union:")
    #print(json_data)

    #with open('entries_con_classify_elevation.txt', 'w') as f:
    with open('entries_con_classify_elevation_ordenada.txt', 'w') as f:
        f.write(json_data)

    return entries



def group_tiles_by_height_parallel(data, num_processes=4, chunk_size=5):
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
    start_time = time.time()

    entries=group_tiles_by_height(data)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de group_tiles_by_height: {elapsed_time:.4f} segundos")

    start_time = time.time()
        
    with Pool(num_processes) as p:
        results = p.imap_unordered(process_union, entries, chunksize=chunk_size)
        
        new_features = [item for sublist in results for item in sublist]
    

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de process_union paralelizando: {elapsed_time:.4f} segundos")


    grouped_data = {
        "type": "FeatureCollection",
        "features": new_features
    }

    json_data = json.dumps(grouped_data, default=lambda obj: str(obj), indent=4)
    #print("Datos entries enviados a process_union:")
    #print(json_data)

    #with open('entries_paralizada_con_classify_elevation.txt', 'w') as f:
    with open('entries_paralizada_con_classify_elevation_ordenada.txt', 'w') as f:
        f.write(json_data)


    return grouped_data

def group_tiles_by_height_sin_parallel(data):
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
    start_time = time.time()

    entries=group_tiles_by_height(data)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de group_tiles_by_height: {elapsed_time:.4f} segundos")

    start_time = time.time()
    #  
    # Procesa cada entrada secuencialmente
    new_features = []
    for entry in entries:
        result = process_union(entry)  # Llama a process_union para cada entrada
        new_features.extend(result)  # Agrega los resultados a la lista
  
    #

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de process_union sin paralelizar: {elapsed_time:.4f} segundos")

    grouped_data = {
        "type": "FeatureCollection",
        "features": new_features
    }
    
    json_data = json.dumps(grouped_data, default=lambda obj: str(obj), indent=4)
    #print("Datos entries enviados a process_union:")
    #print(json_data)

    #with open('entries_sin paralelizar_con_classify_elevation.txt', 'w') as f:
    with open('entries_sin_paralizar_con_classify_elevation_ordenada.txt', 'w') as f:
        f.write(json_data)


    return grouped_data


