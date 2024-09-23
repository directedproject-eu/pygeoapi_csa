# =================================================================
# Copyright (C) 2024 by 52 North Spatial Information Research GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =================================================================
import inspect
import os.path

from quart import Quart, request, Request, send_from_directory
from pygeoapi import flask_app
from pygeoapi.flask_app import API_RULES, CONFIG, api_, OPENAPI
from quart_cors import cors
from werkzeug.datastructures import MultiDict

from api import *
from routes.edr import edr
from routes.stac import stac
from routes.collections import collections
from routes.coverages import coverage
from routes.csa import csa
from routes.processes import oapip


# makes request args modifiable
class ModifiableRequest(Request):
    dict_storage_class = MultiDict
    parameter_storage_class = MultiDict


class CustomQuart(Quart):
    request_class = ModifiableRequest


APP = CustomQuart(__name__,
                  static_folder=os.path.join(os.path.dirname(inspect.getmodule(api_).__file__), "static"),
                  static_url_path='/static')

APP.config['QUART_CORS_ALLOW_ORIGIN'] = os.environ.get("CORS_ALLOW_ORIGIN") or ""
APP.config['QUART_CORS_ALLOW_CREDENTIALS'] = os.environ.get("CORS_ALLOW_CREDENTIALS")
APP.config['QUART_CORS_ALLOW_METHODS'] = os.environ.get("CORS_ALLOW_METHODS")
APP.config['QUART_CORS_ALLOW_HEADERS'] = os.environ.get("CORS_ALLOW_HEADERS")
APP.config['QUART_CORS_EXPOSE_HEADERS'] = os.environ.get("CORS_EXPOSE_HEADERS")
APP.config['QUART_CORS_MAX_AGE'] = os.environ.get("CORS_MAX_AGE")

APP = cors(APP)

APP.url_map.strict_slashes = API_RULES.strict_slashes
APP.config['JSONIFY_PRETTYPRINT_REGULAR'] = CONFIG['server'].get('pretty_print', False)

APP.register_blueprint(csa)

# TODO: make this configurable, only import required/configured
APP.register_blueprint(edr)
APP.register_blueprint(stac)
APP.register_blueprint(oapip)
APP.register_blueprint(coverage)
APP.register_blueprint(collections)


@APP.route('/')
async def landing_page():
    request.collection = ""
    return await to_response(await csapi_.landing(request))


@APP.route('/assets/<path:filename>')
async def assets(filename):
    request.collection = None
    return await send_from_directory("templates/connected-systems/assets", filename)


@APP.route('/openapi')
def openapi():
    request.collection = None
    return flask_app.openapi()


@APP.route('/conformance')
async def conformance():
    request.collection = None
    return await to_response(await csapi_.conformance(request))


@APP.before_serving
async def init_db():
    """ Initialize peristent database/provider connections """
    if csapi_.provider_part1:
        await csapi_.provider_part1.open()
    if csapi_.provider_part2:
        await csapi_.provider_part2.open()


@APP.after_serving
async def close_db():
    """ Clean exit database/provider connections """
    if csapi_.provider_part1:
        await csapi_.provider_part1.close()
    if csapi_.provider_part2:
        await csapi_.provider_part2.close()


def run():
    ## Only used in local development - gunicorn is used for production
    os.environ["PYGEOAPI_CONFIG"] = "pygeoapi-config.yml"
    os.environ["PYGEOAPI_OPENAPI"] = "openapi-config-csa.yml"
    APP.run(debug=False,
            host=api_.config['server']['bind']['host'],
            port=api_.config['server']['bind']['port'])


if __name__ == "__main__":
    run()
