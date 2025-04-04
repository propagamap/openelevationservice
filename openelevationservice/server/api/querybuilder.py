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

from openelevationservice.server.api.elevation_query_parallel import POLYGON_COLORING_ELEVATION_QUERY, group_tiles_by_height_without_parallel 

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


def polygon_coloring_elevation_without_parallel(geometry):
    """
    Processes elevation data in parallel for a polygon geometry and returns a JSON.

    :param geometry: Input 2D polygon geometry to process.
    :type geometry: Shapely geometry

    :raises InvalidUsage: If the geometry processing or query fails.

    :returns: A tuple containing:
              - Feature collection (JSON) enriched with elevation data.
              - List with minimum and maximum elevation.
              - Average elevation in the polygon.
    :rtype: tuple(dict, list[float], float)
    """

    if geometry.geom_type != 'Polygon':
        raise InvalidUsage(400, 4002, f"Needs to be a Polygon, not a {geometry.geom_type}!")

    polygon = str(geometry)  
    session = db.get_session()

    try:
        
        result = session.execute(POLYGON_COLORING_ELEVATION_QUERY, {"polygon": polygon})
        row = result.fetchone()

        if not row:
            raise InvalidUsage(404, 4002, "No elevation data was returned for the specified geometry.")

        features_collection, min_height, max_height, avg_height = row

        features_collection = group_tiles_by_height_without_parallel(
            features_collection,
            min_height,
            max_height,
            num_ranges=23
        )

    except InvalidUsage as exc:
        
        raise exc
    except Exception as e:
        
        raise InvalidUsage(500, 4003, f"An error occurred while processing the geometry: {str(e)}")

    return features_collection, [min_height, max_height], avg_height


def polygon_elevation_sql(geometry, dataset):
    """
    Performs PostGIS query to enrich a polygon geometry.
    
    :param geometry: Input 2D polygon to be enriched with elevation
    :type geometry: Shapely geometry
    
    :param dataset: Elevation dataset to use for querying
    :type dataset: string
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description. 
        
    :returns: List of tuples containing (longitude, latitude, elevation).
    :rtype: list of tuples (float, float, float)
    """
    
    if geometry.geom_type == 'Polygon':

        session = db.get_session()

        POLYGON_ELEVATION_QUERY   = """
            WITH polygon_geom AS (
            SELECT ST_SetSRID(
                    ST_GeomFromText(:wkt_polygon), 4326
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
                SELECT 
                    (ST_PixelAsCentroids(cr.clipped_rast)).geom AS pixel_geom,
                    (ST_PixelAsCentroids(cr.clipped_rast)).val AS pixel_value
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

        result_points = session.execute(text(POLYGON_ELEVATION_QUERY), {"wkt_polygon": geometry.wkt}).fetchall()
            
    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))

    if result_points == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_points

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
