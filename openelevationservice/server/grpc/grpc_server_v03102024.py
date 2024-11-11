from concurrent import futures
from sqlalchemy.exc import SQLAlchemyError
from openelevationservice.server.api import querybuilder, views
from openelevationservice.server.api.api_exceptions import InvalidUsage
from openelevationservice.server.utils import convert
import grpc
from grpc_reflection.v1alpha import reflection
from . import openelevation_pb2 as defs
from . import openelevation_pb2_grpc
from shapely import wkt

#AAOR-Import
import time
from openelevationservice.server.api import direct_queries_hanli_multiple_valores
from openelevationservice.server.api import direct_queries_hanli_multiple_valores_modificada_hanli_cmt
from openelevationservice.server.api import direct_queries_hanli_multiple_valores_original_hanli_cmt
#from openelevationservice.server.api import direct_queries_hanli
import json
#AAOR-Fin Import

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

    ##Original code for the AreaPointsElevation function
    # @handle_exceptions
    # def AreaPointsElevation(self, request, context):
    #     geom = convert.polygon_to_geometry(self._format_area_request(request))
    #     geom_queried = querybuilder.polygon_elevation(geom, 'polygon', 'srtm')
    #     geom_shaped = wkt.loads(geom_queried)
        
    #     result = []
    #     for point in list(geom_shaped.coords):
    #         result.append(defs.LatLonElevation(
    #             lon=point[0],
    #             lat=point[1],
    #             elevation=int(point[2])
    #         ))
    
    #     return defs.AreaPointsResponse(points=result)
    ##Fin-Original code for the AreaPointsElevation function


    ##Start-Modified AAOR code for AreaPointsElevation function
    @handle_exceptions
    def AreaPointsElevation(self, request, context):
        print("AreaPointsElevation--")
        geom = convert.polygon_to_geometry(self._format_area_request(request))
        geom_queried = querybuilder.polygon_elevation_sql_simplificada_2_smt(geom, 'polygon', 'srtm')
        
        result = []
        for point in list(geom_queried):
            result.append(defs.LatLonElevation(
                lon=point[0],
                lat=point[1],
                elevation=int(point[2])
            ))
        
        return defs.AreaPointsResponse(points=result)
 ##End-Modified AAOR code for AreaPointsElevation function
    
    
    def _create_proto_geo_polygon(self, coordinates):
        return defs.Area(boundaries=[
            defs.LineString(points=[
                defs.LatLon(
                    lon=point[0],
                    lat=point[1],
                ) for point in bondary
            ]) for bondary in coordinates            
        ])



    ###Start-Original code for the AreaRangesElevation function
    # @handle_exceptions
    # def AreaRangesElevation(self, request, context):
    #     geom = convert.polygon_to_geometry(self._format_area_request(request))
    #     collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation(geom, 'srtm')
        
    #     result = []
    #     for feature in collection_queried['features']:
    #         heightBase = int(feature['properties']['heightBase'])
    #         if feature['geometry']['type'] == 'Polygon':
    #             result.append(defs.UnitedArea(
    #                 baseElevation=heightBase,
    #                 area=self._create_proto_geo_polygon(feature['geometry']['coordinates']),
    #             ))
    #         else:
    #             for polygon in feature['geometry']['coordinates']:
    #                 result.append(defs.UnitedArea(
    #                     baseElevation=heightBase,
    #                     area=self._create_proto_geo_polygon(polygon),
    #                 ))
        
    #     return defs.AreaRangesResponse(
    #         unions=result,
    #         minElevation=range_queried[0],
    #         maxElevation=range_queried[1],
    #         avgElevation=avg_queried,
    #     )
    ###End-Original code for the AreaRangesElevation function

    ###Start-Modified AAOR for the AreaRangesElevation function
    # @handle_exceptions
    # def AreaRangesElevation(self, request, context):
    #     geom = convert.polygon_to_geometry(self._format_area_request(request))
    #     print("geom")
    #     print(geom)
    #     collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation(geom, 'srtm')
        
    #     result = []
    #     for feature in collection_queried['features']:
    #         heightBase = int(feature['properties']['heightBase'])
    #         if feature['geometry']['type'] == 'Polygon':
    #             result.append(defs.UnitedArea(
    #                 baseElevation=heightBase,
    #                 area=self._create_proto_geo_polygon(feature['geometry']['coordinates']),
    #             ))
    #         else:
    #             for polygon in feature['geometry']['coordinates']:
    #                 result.append(defs.UnitedArea(
    #                     baseElevation=heightBase,
    #                     area=self._create_proto_geo_polygon(polygon),
    #                 ))
        
    #     return defs.AreaRangesResponse(
    #         unions=result,
    #         minElevation=range_queried[0],
    #         maxElevation=range_queried[1],
    #         avgElevation=avg_queried,
    #     )
    ###End-Modified AAOR code for the AreaRangesElevation function


    ###Start-Original code for AreaRangesElevation with time measurement 
    @handle_exceptions
    def AreaRangesElevation(self, request, context):
        print("AreaRangesElevation-con medicion de tiempoooo")
        inicio_coloring_totaltiempo_transcurrido_coloring_total = time.perf_counter() #AAOR

        geom = convert.polygon_to_geometry(self._format_area_request(request))
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_new_2(geom, 'srtm')#AAOR
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_new(geom, 'srtm')#AAOR
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_modified(geom, 'srtm')#AAOR
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation(geom, 'srtm')#original

        ##############originalllll
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_cmt(geom, 'srtm')#original
        # collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation(geom, 'srtm')#original
        # print(f"Altura mínima: {range_queried[0]}, Altura máxima: {range_queried[1]}, Altura promedio: {avg_queried}")
        # inicio_result = time.perf_counter() #AAOR
        # result = []
        # for feature in collection_queried['features']:
        #     heightBase = int(feature['properties']['heightBase'])
        #     if feature['geometry']['type'] == 'Polygon':
        #         result.append(defs.UnitedArea(
        #             baseElevation=heightBase,
        #             area=self._create_proto_geo_polygon(feature['geometry']['coordinates']),
        #         ))
        #     else:
        #         for polygon in feature['geometry']['coordinates']:
        #             result.append(defs.UnitedArea(
        #                 baseElevation=heightBase,
        #                 area=self._create_proto_geo_polygon(polygon),
        #             ))
        # print(" ")

        # fin_result = time.perf_counter() #AAOR
        # tiempo_transcurrido_result = fin_result - inicio_result#AAOR
        # print(f"tiempo_transcurrido_result: {tiempo_transcurrido_result:.6f} segundos")#AAOR
        
        # fin_coloring_totaltiempo_transcurrido_coloring_total = time.perf_counter() #AAOR
        # tiempo_transcurrido_coloring_total = fin_coloring_totaltiempo_transcurrido_coloring_total - inicio_coloring_totaltiempo_transcurrido_coloring_total#AAOR
        # print(f"Tiempo de ejecución_coloring_total: {tiempo_transcurrido_coloring_total:.6f} segundos")#AAOR

        # return defs.AreaRangesResponse(
        #     unions=result,
        #     minElevation=int(range_queried[0]),
        #     maxElevation=int(range_queried[1]),
        #     avgElevation=avg_queried,
        # )
        #####Fin - originalllll


        
        #####Inicia - Hanli
        # collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_based_hanli(geom)#based_hanli
        # #collection_queried = querybuilder.polygon_coloring_elevation_based_hanli(geom)#based_hanli
        # #print("collection_queried ",collection_queried)
        # inicio_result = time.perf_counter() #AAOR

        # # result = []
        # # height_values = []

        # # for feature in collection_queried['features']:
        # #     heightBase = int(feature['properties']['height'])
        # #     height_values.append(heightBase)  # Recopilamos los valores de altura
        # #     #geometry_type = feature['geometry']['type']
            

        # #     if feature['geometry']['type'] == 'Polygon':
        # #         result.append(defs.UnitedArea(
        # #             baseElevation=heightBase,
        # #             area=self._create_proto_geo_polygon(feature['geometry']['coordinates']),
        # #         ))
        # #     else: 
        # #         for polygon in feature['geometry']['coordinates']:
        # #             result.append(defs.UnitedArea(
        # #                 baseElevation=heightBase,
        # #                 area=self._create_proto_geo_polygon(polygon),
        # #             ))

        # ###################

        # result = []
        # height_values = []

        # for feature in collection_queried['features']:
        #     heightBase = int(feature['properties']['height'])
        #     height_values.append(heightBase)  # Recopilamos los valores de altura

        #     # Convertimos la geometría de string a JSON
        #     geometry = json.loads(feature['geometry'])

        #     # Procesamos el tipo de geometría
        #     if geometry['type'] == 'Polygon':
        #         result.append(defs.UnitedArea(
        #             baseElevation=heightBase,
        #             area=self._create_proto_geo_polygon(geometry['coordinates']),
        #         ))
        #     else:
        #         for polygon in geometry['coordinates']:
        #             result.append(defs.UnitedArea(
        #                 baseElevation=heightBase,
        #                 area=self._create_proto_geo_polygon(polygon),
        #             ))

            

        # # Ahora, calculamos la altura mínima, máxima y promedio
        # # if height_values:
        # #     min_height = min(height_values)
        # #     max_height = max(height_values)
        # #     avg_height = sum(height_values) / len(height_values)
        # # else:
        # #     min_height = max_height = avg_height = None

        # # print(" ")
        # # print(f"Altura result: {result}")
        # # print(" ")
        # # print(f"Altura mínima: {min_height}")
        # # print(f"Altura máxima: {max_height}")
        # # print(f"Altura promedio: {avg_height}")

        # fin_result = time.perf_counter() #AAOR
        # tiempo_transcurrido_result = fin_result - inicio_result#AAOR
        # print(f"tiempo_transcurrido_result: {tiempo_transcurrido_result:.6f} segundos")#AAOR
        
        # fin_coloring_totaltiempo_transcurrido_coloring_total = time.perf_counter() #AAOR
        # tiempo_transcurrido_coloring_total = fin_coloring_totaltiempo_transcurrido_coloring_total - inicio_coloring_totaltiempo_transcurrido_coloring_total#AAOR
        # print(f"Tiempo de ejecución_coloring_total: {tiempo_transcurrido_coloring_total:.6f} segundos")#AAOR

        
        # # return defs.AreaRangesResponse(
        # #     unions=result,
        # #     minElevation=int(min_height),
        # #     maxElevation=int(max_height),
        # #     avgElevation=avg_height,
        # # )

        # return defs.AreaRangesResponse(
        #     unions=result,
        #     minElevation=int(range_queried[0]),
        #     maxElevation=int(range_queried[1]),
        #     avgElevation=avg_queried,
        # )
        #####Fin - Hanli
    ######End-Original code for AreaRangesElevation with time measurement


    ###Start-Analysis of the Hanli-1 code
    # @handle_exceptions
    # def AreaRangesElevation(self, request, context):
    #     print("Codigo de hanli")

    #     inicio_coloring_totaltiempo_transcurrido_coloring_total_hanli = time.perf_counter()
    #     geom = convert.polygon_to_geometry(self._format_area_request(request))
    #     print("geom 1")
    #     print(geom)
        
    #     #print(direct_queries_hanli.main(geom))
    #     direct_queries_hanli.main(geom)
    #     fin_coloring_totaltiempo_transcurrido_coloring_total_hanli = time.perf_counter()
    #     tiempo_transcurrido_coloring_total_hanli = fin_coloring_totaltiempo_transcurrido_coloring_total_hanli - inicio_coloring_totaltiempo_transcurrido_coloring_total_hanli#AAOR
    #     print(f"Tiempo de ejecución_coloring_total_hanli: {tiempo_transcurrido_coloring_total_hanli:.6f} segundos")
    ###End-Analysis of the Hanli-1 code


    ###Start-New code for AreaRangesElevation based on the direct_query from Hanli
    # @handle_exceptions
    # def AreaRangesElevation(self, request, context):
    #     print("Call Code Hanli")
    #     geom = convert.polygon_to_geometry(self._format_area_request(request))
    #     #geojson, min_height, max_height, avg_height=direct_queries_hanli_multiple_valores.main(geom)
    #     geojson, min_height, max_height, avg_height=direct_queries_hanli_multiple_valores_modificada_hanli_cmt.main(geom)
    #     #geojson, min_height, max_height, avg_height=direct_queries_hanli_multiple_valores_original_hanli_cmt.main(geom)
    #     #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_new_based_hanli(geom, 'srtm')
    #     #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation(geom, 'srtm')#original
        
    #     # Imprime o procesa los resultados como desees
    #     #print("GeoJSON FeatureCollection:")
    #     #print(geojson)
    #     #print(f"Minimum height: {min_height}")
    #     #print(f"Maximum height: {max_height}")
    #     #print(f"Average height: {avg_height}")
    ###End-New code for AreaRangesElevation based on the direct_query from Hanli



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
