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
#AAOR: importo time para medir el tiempo
import time


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


    ##AAOR-->AreaPointElevation-Simplificado-con medición de tiempo
    @handle_exceptions
    def AreaPointsElevation_(self, request, context):
        print('grpc_server_original_con medición de tiempo')

        inicio_todo = time.perf_counter() 
        print('Comentario AAOR_1',request)
        print('Comentario AAOR_1_T',type(request))
        
        inicio_convert = time.perf_counter() #AAOR
        geom = convert.polygon_to_geometry(self._format_area_request(request))
        print('Comentario AAOR_1 geom',type(geom))
        print('Comentario AAOR_1 geom',geom)
        fin_convert = time.perf_counter() #AAOR
       
        inicio_queried = time.perf_counter() #AAOR
        #geom_queried = querybuilder.polygon_elevation_sql_simplificada_1(geom, 'polygon', 'srtm')
        geom_queried = querybuilder.polygon_elevation_sql_simplificada_2(geom, 'polygon', 'srtm')
        #geom_queried = querybuilder.polygon_elevation_orm_simplificada(geom, 'polygon', 'srtm')
        print('geom_queried',type(geom_queried))
        #print('geom_queried',geom_queried[0:160])
        #geom_queried = querybuilder.polygon_elevation_ref(geom, 'polygon', 'srtm')
        #geom_queried = querybuilder.polygon_elevation__(geom, 'polygon', 'srtm')
        fin_queried = time.perf_counter() #AAOR
        
        #print('result_points como geom_queried',geom_queried)

        #geom_shaped = wkt.loads(geom_queried) 

        inicio_result = time.perf_counter() #AAOR (4)-->Sólo este proceso al simplificar
        result = []
        for point in list(geom_queried):
            result.append(defs.LatLonElevation(
                lat=point[0],
                lon=point[1],
                elevation=int(point[2])
            ))
        fin_result = time.perf_counter() #AAOR

        #print('result',result)
        fin_todo = time.perf_counter()#AAOR

        tiempo_transcurrido_todo = fin_todo - inicio_todo#AAOR
        tiempo_transcurrido_convert = fin_convert - inicio_convert#AAOR
        tiempo_transcurrido_queried = fin_queried - inicio_queried#AAOR
        #tiempo_transcurrido_queried_ref = fin_queried_ref - inicio_queried_ref#AAOR
        
        #tiempo_transcurrido_shaped_ref = fin_shaped_ref - inicio_shaped_ref#AAOR
        tiempo_transcurrido_result = fin_result - inicio_result#AAOR
        print('\n')#AAOR
        print(f"Tiempo de ejecución_todooooooooo: {tiempo_transcurrido_todo:.6f} segundos")#AAOR
        print(f"Tiempo de ejecución_convert: {tiempo_transcurrido_convert:.6f} segundos")#AAOR
        print(f"Tiempo de ejecución_queried: {tiempo_transcurrido_queried:.6f} segundos")#AAOR
        #print(f"Tiempo de ejecución_queried_ref: {tiempo_transcurrido_queried_ref:.6f} segundos")#AAOR
        
        #print(f"Tiempo de ejecución_shaped_ref: {tiempo_transcurrido_shaped_ref:.6f} segundos")#AAOR
        print(f"Tiempo de ejecución_result: {tiempo_transcurrido_result:.6f} segundos")#AAOR
        print(f"Retardo del proceso sin el queried: {tiempo_transcurrido_todo - tiempo_transcurrido_queried:.6f} segundos")#AAOR 
        
        return defs.AreaPointsResponse(points=result)
    ##AAOR-->Fin AreaPointsElevation-Simplificado-con medición de tiempo

    ##AAOR-->AreaPointElevation-Simplificado-sin medición de tiempo
    @handle_exceptions
    def AreaPointsElevation(self, request, context):
        print('grpc_server_modificada_sin medición de tiempo')

        inicio_todo = time.perf_counter() 

        geom = convert.polygon_to_geometry(self._format_area_request(request))

        geom_queried = querybuilder.polygon_elevation_sql_simplificada_2_smt(geom, 'polygon', 'srtm')

        result = []
        for point in list(geom_queried):
            result.append(defs.LatLonElevation(
                lat=point[0],
                lon=point[1],
                elevation=int(point[2])
            ))

        fin_todo = time.perf_counter()#AAOR
        tiempo_transcurrido_todo = fin_todo - inicio_todo#AAOR
        print(f"Tiempo de ejecución_todooooooooo: {tiempo_transcurrido_todo:.6f} segundos")#AAOR

        return defs.AreaPointsResponse(points=result)
    ##AAOR-->Fin AreaPointsElevation-Simplificado-sin medición de tiempo



    
    ##AAOR-->AreaPointsElevation-Original
    @handle_exceptions
    def AreaPointsElevation_(self, request, context):
        inicio_todo = time.perf_counter() 
        print('Comentario AAOR_1',request)
        print('Comentario AAOR_1_T',type(request))

        inicio_convert = time.perf_counter() #AAOR
        geom = convert.polygon_to_geometry(self._format_area_request(request))#(1)
        #print('Comentario AAOR_1 geom',type(geom))
        print('Comentario AAOR_1 geom',geom)
        fin_convert = time.perf_counter() #AAOR



        inicio_queried = time.perf_counter() #AAOR
        geom_queried = querybuilder.polygon_elevation_original(geom, 'polygon', 'srtm')#(2)-->llamada al script qurybuilder.py
        print('geom_queried',type(geom_queried))
        #print('geom_queried',geom_queried[0:160])
        #geom_queried = querybuilder.polygon_elevation_ref(geom, 'polygon', 'srtm')
        #geom_queried = querybuilder.polygon_elevation__(geom, 'polygon', 'srtm')
        fin_queried = time.perf_counter() #AAOR

        inicio_shaped = time.perf_counter() #AAOR
        geom_shaped = wkt.loads(geom_queried)#(3)->proceso
        #print('geom_queried',type(geom_shaped.coords))
        #print('geom_queried',geom_shaped.coords[0], geom_shaped.coords[1],geom_shaped.coords[2],geom_shaped.coords[3])
        #print('geom_queried',geom_shaped)
        fin_shaped = time.perf_counter() #AAOR




        #inicio_queried_ref = time.perf_counter() #AAOR
        #geom_queried_ref= querybuilder.polygon_elevation_to_getModel(geom, 'polygon', 'srtm')
        #geom_queried_ref= querybuilder.polygon_elevation_optimizada(geom, 'polygon', 'srtm')
        #geom_queried_ref= querybuilder.polygon_elevation_ref(geom, 'polygon', 'srtm')
        #geom_queried_ref= querybuilder.polygon_elevation_ref_sin_mt(geom, 'polygon', 'srtm')
        #fin_queried_ref = time.perf_counter() #AAOR

        #inicio_shaped_ref = time.perf_counter() #AAOR
        #geom_shaped_ref = wkt.loads(geom_queried_ref)
        #fin_shaped_ref = time.perf_counter() #AAOR

        
        
        inicio_result = time.perf_counter() #AAOR (4)-->proceso repetido
        result = []
        for point in list(geom_shaped.coords):
            result.append(defs.LatLonElevation(
                lon=point[0],
                lat=point[1],
                elevation=int(point[2])
            ))
        fin_result = time.perf_counter() #AAOR


        fin_todo = time.perf_counter()#AAOR

        tiempo_transcurrido_todo = fin_todo - inicio_todo#AAOR
        tiempo_transcurrido_convert = fin_convert - inicio_convert#AAOR
        tiempo_transcurrido_queried = fin_queried - inicio_queried#AAOR
        #tiempo_transcurrido_queried_ref = fin_queried_ref - inicio_queried_ref#AAOR
        tiempo_transcurrido_shaped = fin_shaped - inicio_shaped#AAOR
        #tiempo_transcurrido_shaped_ref = fin_shaped_ref - inicio_shaped_ref#AAOR
        tiempo_transcurrido_result = fin_result - inicio_result#AAOR
        print('\n')#AAOR
        print(f"Tiempo de ejecución_todooooooooo: {tiempo_transcurrido_todo:.6f} segundos")#AAOR
        print(f"Tiempo de ejecución_convert: {tiempo_transcurrido_convert:.6f} segundos")#AAOR
        print(f"Tiempo de ejecución_queried: {tiempo_transcurrido_queried:.6f} segundos")#AAOR
        #print(f"Tiempo de ejecución_queried_ref: {tiempo_transcurrido_queried_ref:.6f} segundos")#AAOR
        print(f"Tiempo de ejecución_shaped: {tiempo_transcurrido_shaped:.6f} segundos")#AAOR
        #print(f"Tiempo de ejecución_shaped_ref: {tiempo_transcurrido_shaped_ref:.6f} segundos")#AAOR
        print(f"Tiempo de ejecución_result: {tiempo_transcurrido_result:.6f} segundos")#AAOR
        print(f"Retardo del proceso sin el queried: {tiempo_transcurrido_todo - tiempo_transcurrido_queried:.6f} segundos")#AAOR

        # print('lo que se retorna finalmente')
        # print('result',type(result))
        # print('result',result[0], result[1], result[2], result[3])  
        
        return defs.AreaPointsResponse(points=result)
    ##AAOR-->Fin AreaPointsElevation-Original
    
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
