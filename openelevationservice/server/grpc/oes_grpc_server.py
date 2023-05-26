from concurrent import futures
from openelevationservice.server.api import querybuilder
from openelevationservice.server.utils import convert
import grpc
from . import openelevation_pb2 as defs
from . import openelevation_pb2_grpc
from shapely import wkt


class OpenElevationServicer(openelevation_pb2_grpc.OpenElevationServicer):
    """Provides methods that implement functionality of route guide server."""

    def PointElevation(self, request, context):
        geometry = convert.point_to_geometry([request.lon, request.lat])
        print(geometry) # input, print
        geom_queried = querybuilder.point_elevation(geometry, 'point', 'srtm')
        print(geom_queried)
        geom_out = wkt.loads(geom_queried)
        print(geom_out)
        return defs.Elevation(value=geom_out[0][2])

    def LineElevation(self, request, context):
        return defs.LineResponse(points=[
            #...
        ])

    def AreaPointsElevation(self, request, context):
        return defs.AreaPointsResponse(points=[
            #...
        ])

    def AreaRangesElevation(self, request, context):
        return defs.AreaRangesResponse(
            unions=[
                #...
            ],
            minElevation=0,#...,
            maxElevation=0#...
        )


def serve(port_url):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    openelevation_pb2_grpc.add_OpenElevationServicer_to_server(
        OpenElevationServicer(), server)

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


if __name__ == '__main__':
    serve('127.0.0.1:5005')
