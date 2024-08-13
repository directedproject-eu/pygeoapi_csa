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

from quart import Quart, request, Request, make_response, send_from_directory
from pygeoapi import flask_app
from pygeoapi.flask_app import API_RULES, CONFIG, api_, OPENAPI
from quart_cors import cors
from werkzeug.datastructures import MultiDict

from api import *


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

PLUGINS["provider"]["toardb"] = "provider.toardb_csa.ToarDBProvider"
PLUGINS["provider"]["ElasticSearchConnectedSystems"] = \
    "provider.part1.elasticsearch.ConnectedSystemsESProvider"
PLUGINS["provider"]["TimescaleDBConnectedSystems"] = \
    "provider.part2.timescaledb.ConnectedSystemsTimescaleDBProvider"

csapi_ = CSAPI(CONFIG, OPENAPI)


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


@APP.route('/collections')
@APP.route('/collections/<path:collection_id>')
async def collections(collection_id: str = None):
    request.collection = None
    """
    OGC API collections endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    # TODO(specki): add compatibility with AsyncAPIRequest to enable non-csa collections
    # Overwrite original request format with json so we can parse response in wrapper
    # original_format = None
    # if "f" in request.args:
    #    original_format = request.args["f"]
    # request.args["f"] = "json"
    # response = await csapi_.get_collections(request,
    #                                        api_.describe_collections(request, collection_id),
    #                                        original_format,
    #                                        collection_id)
    response = await csapi_.get_collections(request,
                                            ({}, HTTPStatus.NOT_FOUND, ""),
                                            request.args["f"] if "f" in request.args else None,
                                            collection_id)
    return await to_response(response)


@APP.route('/collections/<path:collection_id>/items')
@APP.route('/collections/<path:collection_id>/items/<path:item_id>')
async def collection_items(collection_id: str, item_id: str = None):
    request.collection = None
    # TODO: what if a collection contains Non-CSA Entities as well as CSA entities?
    # TODO: For now assume that collections are either CSA or not

    response_headers, response_code, response_body = None, None, None
    if item_id:
        response_headers, response_code, response_body = api_.get_collection_item(request, collection_id, item_id)
    else:
        response_headers, response_code, response_body = api_.get_collection_items(request, collection_id)

    # Check CSA for matching collection
    if response_code != 200:
        response_headers, response_code, response_body = csapi_.get_collection_items(request,
                                                                                     collection_id,
                                                                                     item_id)
    return await to_response((response_headers, response_code, response_body))


@APP.route('/connected-systems/')
async def csa_catalog_root():
    request.collection = None
    """
    Connected Systems API root endpoint

    :returns: HTTP response
    """
    return await to_response(await csapi_.overview(request))


@APP.route('/systems', methods=['GET', 'POST'])
@APP.route('/systems/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def systems_path(path=None):
    request.collection = "systems"
    return await _default_handler(path, EntityType.SYSTEMS)


@APP.route('/systems/<path:path>/subsystems', methods=['GET', 'POST'])
@APP.route('/systems/<path:path>/deployments', methods=['GET'])
@APP.route('/systems/<path:path>/samplingFeatures', methods=['GET', 'POST'])
@APP.route('/systems/<path:path>/datastreams', methods=['GET', 'POST'])
async def systems_subpath(path=None):
    collection = request.path.split('/')[-1]
    request.collection = collection
    if request.method == 'GET':
        match collection:
            case "subsystems":
                return await to_response(await csapi_.get(request, EntityType.SYSTEMS, ("parent", path)))
            case "deployments":
                return await to_response(await csapi_.get(request, EntityType.DEPLOYMENTS, ("system", path)))
            case "samplingFeatures":
                return await to_response(await csapi_.get(request, EntityType.SAMPLING_FEATURES, ("system", path)))
            case "datastreams":
                return await to_response(await csapi_.get(request, EntityType.DATASTREAMS, ("system", path)))
    elif request.method == 'POST':
        match collection:
            case "subsystems":
                return await to_response(await csapi_.post(request, EntityType.SYSTEMS, ("parent", path)))
            case "samplingFeatures":
                return await to_response(await csapi_.post(request, EntityType.SAMPLING_FEATURES, ("system", path)))
            case "datastreams":
                return await to_response(await csapi_.post(request, EntityType.DATASTREAMS, ("system", path)))


@APP.route('/procedures', methods=['GET', 'POST'])
@APP.route('/procedures/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def procedures_path(path=None):
    request.collection = "procedures"
    return await _default_handler(path, EntityType.PROCEDURES)


@APP.route('/deployments', methods=['GET', 'POST'])
@APP.route('/deployments/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def deployments_path(path=None):
    request.collection = "deployments"
    return await _default_handler(path, EntityType.DEPLOYMENTS)


@APP.route('/samplingFeatures', methods=['GET'])
@APP.route('/samplingFeatures/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def properties_path(path=None):
    request.collection = "samplingFeatures"
    return await _default_handler(path, EntityType.SAMPLING_FEATURES)


@APP.route('/properties', methods=['GET', 'POST'])
@APP.route('/properties/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def properties_subpath(path=None):
    request.collection = "properties"
    return await _default_handler(path, EntityType.PROPERTIES)


@APP.route('/datastreams', methods=['GET'])
@APP.route('/datastreams/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def datastreams_path(path=None):
    request.collection = "datastreams"
    return await _default_handler(path, EntityType.DATASTREAMS)


async def _default_handler(path, entity_type):
    match request.method:
        case "GET":
            if path is not None:
                return await to_response(await csapi_.get(request, entity_type, ("id", path)))
            else:
                return await to_response(await csapi_.get(request, entity_type))
        case "PATCH":
            return await to_response(await csapi_.patch(request, entity_type, ("id", path)))
        case "POST":
            return await to_response(await csapi_.post(request, entity_type))
        case "PUT":
            return await to_response(await csapi_.put(request, entity_type, ("id", path)))
        case "DELETE":
            return await to_response(await csapi_.delete(request, entity_type, ("id", path)))


@APP.route('/datastreams/<path:path>/schema', methods=['GET', 'PUT'])
async def datastreams_schema(path=None):
    request.collection = "schema"
    if request.method == 'GET':
        return await to_response(await csapi_.get(request, EntityType.DATASTREAMS_SCHEMA, ("id", path)))
    else:
        return await to_response(await csapi_.put(request, EntityType.DATASTREAMS_SCHEMA, ("id", path)))


@APP.route('/datastreams/<path:path>/observations', methods=['GET', 'POST'])
async def datastreams_observations(path=None):
    request.collection = "observations"
    if request.method == 'GET':
        return await to_response(await csapi_.get(request, EntityType.OBSERVATIONS, ("datastream", path)))
    else:
        return await to_response(await csapi_.post(request, EntityType.OBSERVATIONS, ("datastream", path)))


@APP.route('/observations', methods=['GET'])
@APP.route('/observations/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def observations_path(path=None):
    request.collection = "observations"
    return await _default_handler(path, EntityType.OBSERVATIONS)


async def to_response(result: APIResponse):
    """
    Creates a Quart Response object and updates matching headers.

    :param result: The result of the API call.
                   This should be a tuple of (headers, status, content).

    :returns: A Response instance.
    """
    return await make_response(result[2], result[1], result[0])


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
