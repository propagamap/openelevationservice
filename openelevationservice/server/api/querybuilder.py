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
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import math
from collections import defaultdict
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

    print("AreaRangesElevation-original")
    
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

##End-Original code for the polygon_coloring_elevation function


#####--------->Query_7:
####Section(1): Query code
###Start-Query code --> Tiles with heights equal to zero are filtered out.
query_7 = text(
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
            )).geom AS geometry,  -- Extrae las celdas como geometría
            (ST_PixelAsPolygons(
                ST_Clip(oes_cgiar.rast, query_geom.geom, 0),
                1, False
            )).val AS height  -- Extrae el valor de elevación
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
###End-Query code --> Tiles with heights equal to zero are filtered out.
####End-Section(1)


####Section(2):Functions
###--->Function that classifies elevations by range
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
###--->End-Function that classifies elevations by range

####--->Function that performs the union used in group_tiles_by_height_parallel
def process_union(input_data):
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
####--->Function that performs the union used in group_tiles_by_height_parallel

####--->Function group_tiles_by_height_parallel --> call the process_union function
def group_tiles_by_height_parallel(data, num_processes=12, chunk_size=5):
    """Groups tiles by height and processes them in parallel."""
    
    groupings = {}
    
    for feature in data["features"]:
        height = feature["properties"]["heightBase"]
        geometry = json.loads(feature["geometry"])  # Convert from string to dict
        polygon = shape(geometry)

        if height not in groupings:
            groupings[height] = []
        
        groupings[height].append(polygon)
    
    entries = [(height, polygons) for height, polygons in groupings.items()]
    
    # Parallel processing
    with Pool(num_processes) as p:
        results = p.imap_unordered(process_union, entries, chunksize=chunk_size)
        
        new_features = [item for sublist in results for item in sublist]

    grouped_data = {
        "type": "FeatureCollection",
        "features": new_features
    }

    return grouped_data


####--->End-Function group_tiles_by_height_parallel --> call the process_union function

####End-Section(2):Functions

####Start-section:Main function-->polygon_coloring_elevation_query_7

#Start-Main function

def polygon_coloring_elevation_query_7_grouping_with_parallel_classification_elevation_by_range(geometry, dataset):
    """Processes elevation data in parallel for a polygon geometry and returns a JSON"""
    print("-------------Consulta 7 (sin adyacencia): polygon_coloring_elevation_consulta_7_agrupando_con_paral_clasif_elevac_por_rango ")
    
    polygon = f"{geometry}"

    session = db.get_session()
    
    try:
        
        result = session.execute(query_7, {"polygon": polygon})
        
        row = result.fetchone()

        if row:

            features_collection, min_height, max_height, avg_height = row
            
            ##1
            features_collection=classify_elevation(features_collection, min_height, max_height, num_ranges=23, no_data_value=-9999)
            
            ##2
            features_collection = group_tiles_by_height_parallel(features_collection, num_processes=4, chunk_size=5)

        else:
            print("No results were returned")

    except Exception as e:
        print(f"Error executing the query: {e}")

    return features_collection, [min_height, max_height], avg_height
    #End-Main function

####End-Section(3):Main function-->polygon_coloring_elevation_query_7


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
