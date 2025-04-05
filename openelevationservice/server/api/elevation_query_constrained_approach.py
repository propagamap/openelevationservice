from sqlalchemy import text
from shapely.geometry import mapping, Polygon, MultiPolygon
import math
from collections import defaultdict
from shapely import from_geojson, unary_union


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

def group_and_union_polygons(features_collection, min_height, max_height, num_ranges):
    """
    Groups polygons by height ranges, performs unions, and returns a GeoJSON FeatureCollection.

    :param features_collection: Input GeoJSON FeatureCollection
    :param min_height: Minimum height value for classification
    :param max_height: Maximum height value for classification
    :param num_ranges: Number of height ranges to create
    :return: GeoJSON FeatureCollection with merged polygons grouped by height
    """
    
    range_div = (max_height - min_height + 1) / num_ranges
    groupings = defaultdict(list)
    
    for feature in features_collection['features']:
        height = feature['properties']['heightBase']
        color_range = math.floor((height - min_height) / range_div)
        height_base = math.ceil(color_range * range_div + min_height)
        groupings[height_base].append(feature['geometry'])
    
    features = []
    
    for height, geojson_strings in groupings.items():
        if not geojson_strings:
            continue
            
        if len(geojson_strings) == 1:
            polygon = from_geojson(geojson_strings[0])
        else:
            geoms = [from_geojson(s) for s in geojson_strings]
            polygon = unary_union(geoms)
       
        if polygon.geom_type == 'MultiPolygon':
            for poly in polygon.geoms:
                features.append({
                    "type": "Feature",
                    "geometry": mapping(poly),
                    "properties": {"heightBase": height}
                })
        else:
            features.append({
                "type": "Feature",
                "geometry": mapping(polygon),
                "properties": {"heightBase": height}
            })
    
    return {
        "type": "FeatureCollection",
        "features": features
    }

