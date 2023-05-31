# -*- coding: utf-8 -*-
from openelevationservice import SETTINGS
from openelevationservice.server.db_import.models import db
from openelevationservice.server.api import api_exceptions
from openelevationservice.server.utils import logger

from flask import Flask, jsonify, g
from flask_cors import CORS
from flasgger import Swagger
import os
import time

#from openelevationservice.server.grpc import oes_grpc_server
#from multiprocessing import Process

log = logger.get_logger(__name__)

def create_app(script_info=None):
    # instantiate the app
    
    app = Flask(__name__)
    
    cors = CORS(app, resources={r"/elevation/*": {"origins": "*"}})

    app.config['SWAGGER'] = {
        'title': 'openelevationservice',
        "swagger_version": "2.0",
        'version': 0.1,
        'uiversion': 3
    }

    # set config
    app_settings = os.getenv('APP_SETTINGS',   'openelevationservice.server.config.ProductionConfig')
    app.config.from_object(app_settings)

    # set up extensions
    db.init_app(app)
    
    provider_details = SETTINGS['provider_parameters']
    log.info("Following provider parameters are active:\n"
              "Host:\t{host}\n"
              "DB:\t{db_name}\n"
              "Table:\t{table_name}\n"
              "User:\t{user_name}".format(**provider_details))

    # register blueprints
    from openelevationservice.server.api.views import main_blueprint
    app.register_blueprint(main_blueprint)

    Swagger(app, template_file='api/oes_post.yaml')

    if "Development" in app_settings:
        @app.before_request
        def before_request():
            g.start = time.time()

        @app.teardown_request
        def teardown_request(exception=None):
            if 'start' in g:
                diff = time.time() - g.start
                log.debug("Request took: {} seconds".format(diff))

    # error handlers
    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({"code": 400, "message": "Bad Request"})
    
    @app.errorhandler(401)
    def unauthorized_page(error):
        return jsonify({"code": 401, "message": "Unauthorized to view page"})

    @app.errorhandler(403)
    def forbidden_page(error):
        return jsonify({"code": 403, "message": "Forbidden page"})

    @app.errorhandler(404)
    def page_not_found(error):
        return jsonify({"code": 404, "message": "Endpoint not found"})
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        return jsonify({"code": 405, 'message': "HTTP Method not allowed"})

    @app.errorhandler(500)
    def server_error_page(error):
        return jsonify({"code": 500, 'message': 'Server error'})

    @app.errorhandler(api_exceptions.InvalidUsage)
    def handle_invalid_usage(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    # shell context for flask cli
    app.shell_context_processor({
        'app': app,
        'db': db}
    )

    # TODO: Allow gRPC server to run in parallel if needed
    # grpc_url = '127.0.0.1:5005'
    # with app.app_context():
    #     oes_grpc_server.serve(grpc_url)
    # log.info("Started gRPC server on {}".format(grpc_url))
    # grpc_process = Process(
    #     target=oes_grpc_server.serve,
    #     args=(grpc_url,)
    # )
    # grpc_process.start()

    return app