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
import sys
#from openelevationservice.server.api import direct_queries_hanli
import json
#AAOR-Fin Import


### Function to calculate the size of the generated file
def calculate_size(obj):
    # Measure the size in bytes of the JSON string
    size_in_bytes = sys.getsizeof(obj)

    # Convert to kilobytes (KB)
    size_in_kb = size_in_bytes / 1024

    # Convert to megabytes (MB)
    size_in_mb = size_in_bytes / (1024 * 1024)

    print(f"File size to transfer from OES to the Frontend in KB: {size_in_kb:.2f} KB")
    print(f"File size to transfer from OES to the Frontend in MB: {size_in_mb:.2f} MB")       
### End Function to calculate the size of the generated file


###Función para calcular el tamaño del archivo generado
def calcular_peso(objeto):
    # Medir el tamaño en bytes de la cadena JSON
    tamano_en_bytes = sys.getsizeof(objeto)

    # Convertir a kilobytes (KB)
    tamano_en_kb = tamano_en_bytes / 1024

    # Convertir a megabytes (MB)
    tamano_en_megas = tamano_en_bytes / (1024 * 1024)

    print(f"Tamaño del archivo a transferir desde OES al Frontend en KB: {tamano_en_kb:.2f} KB")
    print(f"Tamaño del archivo a transferir desde OES al Frontend en MB: {tamano_en_megas:.2f} MB")       
###Fin Función para calcular el tamaño del archivo generado


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
    def AreaPointsElevation_(self, request, context):
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

    ####################### New Parallelization Analysis 03Oct2024 ##################

    #####---->    (1)   ######
    
    ###Start-Original code for the AreaRangesElevation function
    @handle_exceptions
    def AreaRangesElevation_(self, request, context):
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
    ###End-Original code for the AreaRangesElevation function

    #####---->    (2)   ######

    ###Start-QUERY_7
    @handle_exceptions
    def AreaRangesElevation_(self, request, context):
        # print(" ")
        # print("request",request)
        geom = convert.polygon_to_geometry(self._format_area_request(request))
        print(" ")
        #print("geom",geom)
        print("------------->polygon_coloring_elevation_query_7-->(Without Adjacency)")
        inicio = time.perf_counter()

        #####-->OJO:Se dan varios casos donde se paraleliza debido a diferentes librerias y subprocesos de uniones. OJO
        #para producción-pullRequest-->version paralelizando uniones y mejorando-paralelizando subproceso de uniones
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7(geom, 'srtm')


        #Separación de código en el script querybuilder para analizar -->sirve para escribir paper
        #Lista(FP2)
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7_sin_agrupar(geom, 'srtm')
        #Lista(FP3)
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7_agrupando_sin_clasif_elevac_por_rango_sin_paral(geom, 'srtm')
        #Lista(FP4)
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7_agrupando_con_clasif_elevac_por_rango(geom, 'srtm')
        #Lista(FP5)
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7_agrupando_con_paral_sin_clasif_elevac_por_rango(geom, 'srtm')
        #Lista(FP6)--> (mejor opcion)--> Se subira a producción
        collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7_agrupando_con_paral_clasif_elevac_por_rango(geom, 'srtm')
        #Lista(FP7)--> (mejor opcion para grandes extensines de terreno)
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7_agrupando_con_paral_spm_clasif_elevac_por_rango(geom, 'srtm')#-->spm:subproceso de uniones se mejora
        fin = time.perf_counter()
        print(f"Tiempo de ejecución collection_queried y otros valores: {fin - inicio:.6f} segundos")
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_consulta_7_con_explain_analize(geom, 'srtm')
        #print("collection_queried",collection_queried)
        # print("range_queried[0]",range_queried[0])
        # print("range_queried[1]",range_queried[1])
        # print("avg_queried",avg_queried)
             
        inicio_result = time.perf_counter()
        result = []
        for feature in collection_queried['features']:
            geometry = feature['geometry']  # Ya es un dict, no necesita json.loads
            heightBase = int(feature['properties']['heightBase'])
            
            if geometry['type'] == 'Polygon':
                result.append(defs.UnitedArea(
                    baseElevation=heightBase,
                    area=self._create_proto_geo_polygon(geometry['coordinates']),
                ))
            else:
                for polygon in geometry['coordinates']:
                    result.append(defs.UnitedArea(
                        baseElevation=heightBase,
                        area=self._create_proto_geo_polygon(polygon),
                    ))
        fin_result = time.perf_counter()
        print(f"Tiempo de ejecución_result: {fin_result - inicio_result:.6f} segundos")

        ###Star-Measure file size to transfer
        calculate_size(result)
        ###End-Measure file size to transfer


        return defs.AreaRangesResponse(
            unions=result,
            minElevation=int(range_queried[0]),
            maxElevation=int(range_queried[1]),
            avgElevation=avg_queried,
)
    ###End-QUERY_7
    #####END New Parallelization Analysis 03Oct2024

    ###Start-Original-modified-cmt code for the AreaRangesElevation function
    @handle_exceptions
    def AreaRangesElevation(self, request, context):
        geom = convert.polygon_to_geometry(self._format_area_request(request))
        # print(" ")
        # print("geom",geom)
        print("polygon_coloring_elevation_originallL")
        #print("collection_queried",collection_queried)
        inicio = time.perf_counter()
        collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation(geom, 'srtm')
        fin = time.perf_counter()
        print(f"Tiempo de ejecución collection_queried y otros valores: {fin - inicio:.6f} segundos")
        #print("collection_queried",collection_queried)
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_modified_cmt(geom, 'srtm')
        #collection_queried, range_queried, avg_queried = querybuilder.polygon_coloring_elevation_cmt(geom, 'srtm')

        inicio_result = time.perf_counter()         
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
        fin_result = time.perf_counter()
        print(f"Tiempo de ejecución_result: {fin_result - inicio_result:.6f} segundos")

        ###Star-Measure file size to transfer
        calculate_size(result)
        ###End-Measure file size to transfer

        
        return defs.AreaRangesResponse(
            unions=result,
            minElevation=range_queried[0],
            maxElevation=range_queried[1],
            avgElevation=avg_queried,
        )
    ###End-Original-modified-cmt code for the AreaRangesElevation function


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
