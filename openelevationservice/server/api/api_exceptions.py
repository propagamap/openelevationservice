# -*- coding: utf-8 -*-

from openelevationservice.server.api import error_codes


class InvalidUsage(Exception):
    """Provides more detailed description of internal 500 error."""

    def __init__(self, status_code=400, error_code=None, message=None):
        """
        :param status_code: the HTTP status code
        :type status_code: integer

        :param error_code: internal error code
        :type payload: integer
        
        :param message: custom error message
        :type message: string
        """

        Exception.__init__(self)

        if status_code is not None:
            self.status_code = status_code
            
            if message is None:
                message = error_codes[error_code]
            else:
                message = ' '.join([error_codes[error_code],
                                    message])

            self.error = {
                "code": error_code,
                "message": message
            }

    def to_dict(self):
        """converts error to dict"""
        
        rv = dict(self.error or ())
        return rv


def check_grpc_context(grpc_context):
    """
    Checks if the gRPC context is active. If not, raises an InvalidUsage exception.
    
    This function should be called periodically during long-running operations
    to detect if the client has cancelled the request.
    
    :param grpc_context: Optional gRPC context to check for request cancellation.
    :type grpc_context: grpc.ServicerContext or None
    
    :raises InvalidUsage: If the context is not active (request cancelled by client).
    """
    if grpc_context and not grpc_context.is_active():
        raise InvalidUsage(499, 4099, "Request cancelled by client")