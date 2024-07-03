from concurrent import futures
from sqlalchemy.exc import SQLAlchemyError
from openelevationservice.server.api import querybuilder, views
from openelevationservice.server.api.api_exceptions import InvalidUsage
from openelevationservice.server.grpc.direct_queries import extended_area_elevation, stretched_area_elevation
from openelevationservice.server.utils import convert
import grpc
from grpc_reflection.v1alpha import reflection
from . import openelevation_pb2 as defs
from . import openelevation_pb2_grpc
from shapely import wkt

def handle_exceptions(func):
    def wrapper(self, request, context):
        try:
            return func(self, request, context)
        except InvalidUsage as error:
            context.abort(grpc.StatusCode.INTERNAL, error.to_dict().get('message'))
        except SQLAlchemyError as error:
            context.abort(grpc.StatusCode.INTERNAL, 'Could not connect to database.')
        except Exception as error:
            print(error)
            context.abort(grpc.StatusCode.INTERNAL, 'An unexpected error occurred.')
    return wrapper

class OpenElevationServicer(openelevation_pb2_grpc.OpenElevationServicer):
    """Provides methods that implement functionality of route guide server."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @handle_exceptions
    def PointElevation(self, request, context):
        geom = convert.point_to_geometry([request.lon, request.lat])
        geom_queried = querybuilder.point_elevation(geom, 'point', 'srtm')
        geom_shaped = wkt.loads(geom_queried)
        point_3d = list(geom_shaped.coords[0])
        elevation = int(point_3d[2])
        return defs.Elevation(value=elevation)

    @handle_exceptions
    def LineElevation(self, request, context):
        geom = convert.polyline_to_geometry([
            [request.start.lon, request.start.lat],
            [request.end.lon, request.end.lat]
        ])
        geom_queried = querybuilder.line_elevation(geom, 'polyline', 'srtm')
        geom_shaped = wkt.loads(views.zero_len_line_format(geom_queried))
        
        result = []
        for point in list(geom_shaped.coords):
            result.append(defs.LatLonElevation(
                lon=point[0],
                lat=point[1],
                elevation=int(point[2])
            ))

        return defs.LineResponse(points=result)
        
    def _format_area_request(self, request):
        min_lat = request.bottomLeft.lat
        min_lon = request.bottomLeft.lon
        max_lat = request.topRight.lat
        max_lon = request.topRight.lon
        return [
            [min_lon, min_lat],
            [max_lon, min_lat],
            [max_lon, max_lat],
            [min_lon, max_lat],
            [min_lon, min_lat]
        ]

    @handle_exceptions
    def AreaPointsElevation(self, request, context):
        geom = convert.polygon_to_geometry(self._format_area_request(request))
        geom_queried = querybuilder.polygon_elevation(geom, 'polygon', 'srtm')
        geom_shaped = wkt.loads(geom_queried)
        
        result = []
        for point in list(geom_shaped.coords):
            result.append(defs.LatLonElevation(
                lon=point[0],
                lat=point[1],
                elevation=int(point[2])
            ))

        return defs.AreaPointsResponse(points=result)

    @handle_exceptions
    def StretchedAreaElevation(self, request, context):
        bot_left = (request.bottomLeft.lon, request.bottomLeft.lat)
        top_right = (request.topRight.lon, request.topRight.lat)
        stretch_point = (request.stretch.lon, request.stretch.lat)
        geom = stretched_area_elevation(bot_left, top_right, stretch_point)
        
        result = [defs.LatLonElevation(lon=p[0], lat=p[1], elevation=int(p[2])) for p in geom]
        return defs.AreaPointsResponse(points=result)
    
    @handle_exceptions
    def ExtendedAreaElevation(self, request, context):
        bot_left = (request.bottomLeft.lon, request.bottomLeft.lat)
        top_right = (request.topRight.lon, request.topRight.lat)
        extend_points = [(point.lon, point.lat) for point in request.extendPoints]
        geom = extended_area_elevation(bot_left, top_right, extend_points)

        result = [defs.LatLonElevation(lon=p[0], lat=p[1], elevation=int(p[2])) for p in geom]
        return defs.AreaPointsResponse(points=result)
    
    def _create_proto_geo_polygon(self, coordinates):
        return defs.Area(boundaries=[
            defs.LineString(points=[
                defs.LatLon(
                    lon=point[0],
                    lat=point[1],
                ) for point in bondary
            ]) for bondary in coordinates            
        ])

    @handle_exceptions
    def AreaRangesElevation(self, request, context):
        geom = convert.polygon_to_geometry(self._format_area_request(request))
        collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation(geom, 'srtm')
        
        result = []
        for feature in collection_queried['features']:
            heightBase = int(feature['properties']['heightBase'])
            if feature['geometry']['type'] == 'Polygon':
                result.append(defs.UnitedArea(
                    baseElevation=heightBase,
                    area=self._create_proto_geo_polygon(feature['geometry']['coordinates']),
                ))
            else:
                for polygon in feature['geometry']['coordinates']:
                    result.append(defs.UnitedArea(
                        baseElevation=heightBase,
                        area=self._create_proto_geo_polygon(polygon),
                    ))
        
        return defs.AreaRangesResponse(
            unions=result,
            minElevation=range_queried[0],
            maxElevation=range_queried[1],
            avgElevation=avg_queried,
        )

def grpc_serve(port_url):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    openelevation_pb2_grpc.add_OpenElevationServicer_to_server(
        OpenElevationServicer(), server)
        
    SERVICE_NAMES = (
        defs.DESCRIPTOR.services_by_name['OpenElevation'].full_name,
        reflection.SERVICE_NAME,
    )

    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port(port_url)

    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
