from sqlalchemy import text
import json
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.ops import unary_union
from multiprocessing import Pool
import math


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
    
    groupings = {}
    
    for feature in data["features"]:
        height = feature["properties"]["heightBase"]
        geometry = json.loads(feature["geometry"])  
        polygon = shape(geometry)

        if height not in groupings:
            groupings[height] = []
        
        groupings[height].append(polygon)
    
    entries = [(height, polygons) for height, polygons in groupings.items()]
    
    with Pool(num_processes) as p:
        results = p.imap_unordered(process_union, entries, chunksize=chunk_size)
        
        new_features = [item for sublist in results for item in sublist]

    grouped_data = {
        "type": "FeatureCollection",
        "features": new_features
    }

    return grouped_data



