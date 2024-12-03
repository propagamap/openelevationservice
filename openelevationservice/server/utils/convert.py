# -*- coding: utf-8 -*-

from openelevationservice.server.api.api_exceptions import InvalidUsage
from openelevationservice.server.utils import logger

from shapely.geometry import shape, LineString, Point, Polygon

from pyproj import Geod

log = logger.get_logger(__name__)

def geojson_to_geometry(geometry_str):
    """
    Converts GeoJSON to shapely geometries
    
    :param geometry_str: GeoJSON representation to be converted
    :type geometry_str: str
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description.
    
    :returns: Shapely geometry
    :rtype: Shapely geometry
    """
    
    try:
        geom = shape(geometry_str)
    except Exception as e:
        raise InvalidUsage(status_code=400,
                          error_code=4002,
                          message=str(e))
    return geom
    
    
def point_to_geometry(point):
    """
    Converts a point to shapely Point geometries
    
    :param point: coordinates of a point
    :type point: list/tuple
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description.
    
    :returns: Point
    :rtype: shapely.geometry.Point
    """
    
    try:
        geom = Point(point)
    except Exception as e:
        raise InvalidUsage(status_code=400,
                          error_code=4002,
                          message=str(e))
    return geom

def polyline_to_geometry(point_list):
    """
    Converts a list/tuple of coordinates lists/tuples to a shapely LineString.
    
    :param point_list: Coordinates of line to be converted.
    :type point_list: list/tuple of lists/tuples
    
    :raises InvalidUsage: internal HTTP 500 error with more detailed description.
    
    :returns: LineString
    :rtype: shapely.geometry.LineString
    """
    
    try:
        geom = LineString(point_list)
    except Exception as e:
        raise InvalidUsage(status_code=400,
                          error_code=4002,
                          message=str(e))
    return geom

def polygon_to_geometry(point_list):
    try:
        geom = Polygon(point_list)
    except Exception as e:
        raise InvalidUsage(status_code=400,
                          error_code=4002,
                          message=str(e))
    return geom


def calculate_geodesic_area_km2(geom):
    """
    Calculates the geodesic area of a polygon in square kilometers,
    using the WGS84 ellipsoid.

    :param geom: Polygon geometry (EPSG:4326).
    :type geom: shapely.geometry.Polygon

    :raises InvalidUsage: internal HTTP 400 error with a detailed description.

    :returns: Geodesic area in square kilometers.
    :rtype: float
    """
    try:

        geod = Geod(ellps="WGS84")
        
        lon, lat = zip(*geom.exterior.coords)
        
        area_m2, _ = geod.polygon_area_perimeter(lon, lat)
        
        area_km2 = abs(area_m2) / 1e6

    except Exception as e:
        raise InvalidUsage(
            status_code=400,
            error_code=4003, 
            message=str(e)
        )
    
    return area_km2
