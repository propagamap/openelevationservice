import sys
from openelevationservice.server.grpc.grpc_server import grpc_serve
from openelevationservice.server.utils.logger import get_logger

log = get_logger(__name__)

def main():
    """Starts the gRPC server"""
    grpc_url = '0.0.0.0:5050'
    log.info("gRPC server starting on port {}".format(grpc_url))
    grpc_serve(grpc_url)

if __name__ == '__main__':
    main()
