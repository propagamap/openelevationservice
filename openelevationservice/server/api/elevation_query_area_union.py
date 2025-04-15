from sqlalchemy import text
from shapely.geometry import mapping
import math
from collections import defaultdict
from shapely import unary_union

#OLD-Original
# PIXEL_POLYGONS_WITH_HEIGHT_QUERY = text(
#     """
#     WITH query_geom AS (
#     SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
# ),
# pixels AS (
#     SELECT 
#         (ST_PixelAsPolygons(
#             ST_Clip(oes_cgiar.rast, query_geom.geom, -32768),
#             1, False
#         )).geom AS geometry, 
#         (ST_PixelAsPolygons(
#             ST_Clip(oes_cgiar.rast, query_geom.geom, -32768),
#             1, False
#         )).val AS height  
#     FROM query_geom 
#     JOIN oes_cgiar 
#     ON ST_Intersects(oes_cgiar.rast, query_geom.geom)
# )
# SELECT 
#     ST_AsText(geometry) AS wkt,
#     height
# FROM pixels
# WHERE height != -32768;

#     """
# )
#End-OLD-Original

#opcion1
# PIXEL_POLYGONS_WITH_HEIGHT_QUERY = text(
#     """
#     WITH query_geom AS (
#         SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
#     ),
#     pixels AS (
#         SELECT 
#             p.geom AS geometry, 
#             p.val AS height  
#         FROM query_geom 
#         JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, query_geom.geom),
#         LATERAL ST_PixelAsPolygons(
#             ST_Clip(oes_cgiar.rast, query_geom.geom, -32768),
#             1, False
#         ) AS p
#     )
#     SELECT 
#         ST_AsText(geometry) AS wkt,
#         height
#     FROM pixels
#     WHERE height != -32768;
#     """
# )
#End-opcion1

#opcion2
PIXEL_POLYGONS_WITH_HEIGHT_QUERY = text(
    """
    WITH query_geom AS (
        SELECT ST_SetSRID(ST_GeomFromText(:polygon), 4326) AS geom
    ),
    clipped_rasters AS (
        SELECT ST_Clip(oes_cgiar.rast, q.geom, -32768) AS rast
        FROM query_geom q
        JOIN oes_cgiar ON ST_Intersects(oes_cgiar.rast, q.geom)
    ),
    pixels AS (
        SELECT 
            p.geom AS geometry, 
            p.val AS height
        FROM clipped_rasters,
        LATERAL ST_PixelAsPolygons(rast, 1, False) AS p
        WHERE p.val != -32768
    )
    SELECT 
        ST_AsText(geometry) AS wkt,
        height
    FROM pixels;
    """
)
#End-opcion2

def group_and_union_geometries(geometries_by_height, min_height, max_height, num_ranges):
    """
    Groups Shapely geometries by elevation and unions them per range.

    :param geometries_by_height: List of (shapely_geometry, height)
    :param min_height: Minimum elevation value
    :param max_height: Maximum elevation value
    :param num_ranges: Number of color ranges to group by
    :return: GeoJSON FeatureCollection with unioned geometries
    """

    range_div = (max_height - min_height + 1) / num_ranges
    groupings = defaultdict(list)

    for geom, height in geometries_by_height:
        color_range = math.floor((height - min_height) / range_div)
        height_base = math.ceil(color_range * range_div + min_height)
        groupings[height_base].append(geom)

    features = []

    for height, geoms in groupings.items():
        if not geoms:
            continue

        unioned = unary_union(geoms)

        if unioned.geom_type == 'MultiPolygon':
            for poly in unioned.geoms:
                features.append({
                    "type": "Feature",
                    "geometry": mapping(poly),
                    "properties": {"heightBase": height}
                })
        else:
            features.append({
                "type": "Feature",
                "geometry": mapping(unioned),
                "properties": {"heightBase": height}
            })

    return {
        "type": "FeatureCollection",
        "features": features
    }

