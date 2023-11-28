from concurrent import futures
from openelevationservice.server.api import querybuilder, views
from openelevationservice.server.utils import convert
import grpc
from grpc_reflection.v1alpha import reflection
from . import openelevation_pb2 as defs
from . import openelevation_pb2_grpc
from shapely import wkt

class OpenElevationServicer(openelevation_pb2_grpc.OpenElevationServicer):
    """Provides methods that implement functionality of route guide server."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def PointElevation(self, request, context):
            geom = convert.point_to_geometry([request.lon, request.lat])
            geom_queried = querybuilder.point_elevation(geom, 'point', 'srtm')
            geom_shaped = wkt.loads(geom_queried)
            point_3d = list(geom_shaped.coords[0])
            elevation = int(point_3d[2])
            return defs.Elevation(value=elevation)

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
    
    def _create_proto_geo_polygon(self, coordinates):
        return defs.Area(boundaries=[
            defs.LineString(points=[
                defs.LatLon(
                    lon=point[0],
                    lat=point[1],
                ) for point in bondary
            ]) for bondary in coordinates            
        ])

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
    
    # TODO: use correct credentials if needed
    # if '-s' in sys.argv[1:]:
    #     privkey = open('./test_ssl/test_key.pem', 'rb').read()
    #     certchain = open('./test_ssl/test_cert.pem', 'rb').read()
    #     server.add_secure_port(
    #         port_url,
    #         grpc.ssl_server_credentials(
    #             ((privkey, certchain), ),
    #             # root_certificates=None,
    #             # require_client_auth=False
    #         )
    #     )
    # else:
    server.add_insecure_port(port_url)

    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
