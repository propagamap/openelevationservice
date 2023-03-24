# -*- coding: utf-8 -*-

from openelevationservice import SETTINGS
from openelevationservice.server.utils.logger import get_logger
from openelevationservice.server.db_import.models import db, Cgiar
# from openelevationservice.server.utils.custom_func import ST_SnapToGrid
from openelevationservice.server.api.api_exceptions import InvalidUsage

from geoalchemy2.functions import ST_Value, ST_Intersects, ST_X, ST_Y # ST_DumpPoints, ST_Dump, 
from sqlalchemy import func, literal_column
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import array

log = get_logger(__name__)

coord_precision = float(SETTINGS['coord_precision'])
division_limit = 1 / float(SETTINGS['maximum_nodes'])

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
        
    :returns: 3D polygon as GeoJSON or WKT
    :rtype: string
    """
    Model = _getModel(dataset)

    # May be a good idea to make this a parameter from request
    num_ranges = 23
    range_div = 43
    
    if geometry.geom_type == 'Polygon':
        query_geom = db.session \
                            .query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326) \
                            .label('geom')) \
                            .subquery().alias('pGeom')

        result_pixels = db.session \
                            .query(func.DISTINCT(func.ST_PixelAsPolygons(
                                func.ST_Clip(Model.rast, query_geom.c.geom, 0), #.geom
                                1, False))) \
                            .select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))) \
                            .all()
        
        polygon_col, height_col = format_PixelAsGeoms(result_pixels)

        column_set = db.session \
                            .query(polygon_col.label("geometry"),
                                   func.LEAST(func.floor(height_col / range_div), num_ranges).label("colorRange")) \
                            .subquery().alias('columnSet')

        query_features = db.session \
                            .query(func.jsonb_build_object(
                                'type', 'Feature',
                                'geometry', func.ST_AsGeoJson(
                                    func.ST_SimplifyPreserveTopology(func.ST_Union(func.array_agg(
                                        func.ST_ReducePrecision(column_set.c.geometry, 1e-12)
                                    )), 1e-12)
                                ).cast(JSON),
                                'properties', func.json_build_object(
                                    'heightBase', column_set.c.colorRange * range_div,
                                )).label('features') \
                            ).select_from(column_set) \
                            .group_by(column_set.c.colorRange) \
                            .subquery().alias('rfeatures')

        # Return GeoJSON directly in PostGIS
        query_final = db.session \
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
    
    return result_geom


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
        query_geom = db.session \
                            .query(func.ST_SetSRID(func.ST_PolygonFromText(geometry.wkt), 4326) \
                            .label('geom')) \
                            .subquery().alias('pGeom')

        result_pixels = db.session \
                            .query(func.DISTINCT(func.ST_PixelAsPoints(
                                func.ST_Clip(Model.rast, query_geom.c.geom, 0), #.geom
                                1, False))) \
                            .select_from(query_geom.join(Model, ST_Intersects(Model.rast, query_geom.c.geom))) \
                            .all()
        
        point_col, height_col = format_PixelAsGeoms(result_pixels)

        query_points3d = db.session \
                            .query(func.ST_SetSRID(func.ST_MakePoint(ST_X(point_col),
                                                                     ST_Y(point_col),
                                                                     height_col),
                                              4326).label('geom')) \
                            .order_by(ST_X(point_col), ST_Y(point_col)) \
                            .subquery().alias('points3d')

        if format_out == 'geojson':
            # Return GeoJSON directly in PostGIS
            query_final = db.session \
                              .query(func.ST_AsGeoJson(func.ST_Collect(query_points3d.c.geom)))
            
        else:
            # Else return the WKT of the geometry
            query_final = db.session \
                              .query(func.ST_AsText(func.ST_MakeLine(query_points3d.c.geom)))
    else:
        raise InvalidUsage(400, 4002, "Needs to be a Polygon, not a {}!".format(geometry.geom_type))
    
    result_geom = query_final.scalar()

    # Behaviour when all vertices are out of bounds
    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_geom


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
        num_points = db.session \
                        .query(func.ST_NPoints(geometry.wkt)) \
                        .scalar()
        
        if int(num_points) != 2:
            raise InvalidUsage(400, 4002, "Actually, only LineString with exactly 2 points are supported!")
        
        # geometry.bounds = (minX, minY, maxX, maxY)
        lineLen = max(geometry.bounds[2] - geometry.bounds[0], geometry.bounds[3] - geometry.bounds[1])

        query_points2d = db.session \
                            .query(func.ST_SetSRID(func.ST_DumpPoints(func.ST_Union(
                                array([
                                    func.ST_PointN(geometry.wkt, 1),
                                    func.ST_LineInterpolatePoints(
                                        geometry.wkt,
                                        max(min(1, coord_precision / lineLen), division_limit)
                                    ),
                                    func.ST_PointN(geometry.wkt, 2)
                                ])
                            )).geom, 4326).label('geom')).subquery().alias('points2d')

        query_getelev = db.session \
                            .query(func.DISTINCT(query_points2d.c.geom).label('geom'),
                                   ST_Value(Model.rast, query_points2d.c.geom).label('z')) \
                            .select_from(query_points2d) \
                            .join(Model, ST_Intersects(Model.rast, query_points2d.c.geom)) \
                            .subquery().alias('getelevation')

        query_points3d = db.session \
                            .query(func.ST_SetSRID(func.ST_MakePoint(ST_X(query_getelev.c.geom),
                                                                     ST_Y(query_getelev.c.geom),
                                                                     func.coalesce(query_getelev.c.z, 0)),
                                              4326).label('geom')) \
                            .order_by(ST_X(query_getelev.c.geom)) \
                            .subquery().alias('points3d')

        if format_out == 'geojson':
            # Return GeoJSON directly in PostGIS
            query_final = db.session \
                              .query(func.ST_AsGeoJson(func.ST_MakeLine(query_points3d.c.geom))) #ST_SnapToGrid(, coord_precision)
            
        else:
            # Else return the WKT of the geometry
            query_final = db.session \
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
        query_point2d = db.session \
                            .query(func.ST_SetSRID(func.St_PointFromText(geometry.wkt), 4326).label('geom')) \
                            .subquery() \
                            .alias('points2d')
        
        query_getelev = db.session \
                            .query(query_point2d.c.geom,
                                   ST_Value(Model.rast, query_point2d.c.geom).label('z')) \
                            .select_from(query_point2d) \
                            .join(Model, ST_Intersects(Model.rast, query_point2d.c.geom)) \
                            .limit(1) \
                            .subquery().alias('getelevation')
        
        if format_out == 'geojson': 
            query_final = db.session \
                                .query(func.ST_AsGeoJSON(func.ST_MakePoint(ST_X(query_getelev.c.geom),
                                                                           ST_Y(query_getelev.c.geom),
                                                                           func.coalesce(query_getelev.c.z, 0)
                                                                        )))
        else:
            query_final = db.session \
                                .query(func.ST_AsText(func.ST_MakePoint(ST_X(query_getelev.c.geom),
                                                                        ST_Y(query_getelev.c.geom),
                                                                        func.coalesce(query_getelev.c.z, 0)
                                                                    )))
    else:
        raise InvalidUsage(400, 4002, "Needs to be a Point, not {}!".format(geometry.geom_type))
    
    result_geom = query_final.scalar()

    if result_geom == None:
        raise InvalidUsage(404, 4002,
                           'The requested geometry is outside the bounds of {}'.format(dataset))
        
    return result_geom
