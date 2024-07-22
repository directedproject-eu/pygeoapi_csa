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
    "provider.part1.elasticsearch_csa.ConnectedSystemsESProvider"
PLUGINS["provider"]["TimescaleDBConnectedSystems"] = \
    "provider.part2.timescaledb_csa.ConnectedSystemsTimescaleDBProvider"

csapi_ = CSAPI(CONFIG, OPENAPI)


@APP.route('/')
async def landing_page():
    request.collection = ""
    return await get_response(await csapi_.landing_page(request))


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
    return await get_response(await csapi_.conformance(request))


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
    return await get_response(response)


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
    return await get_response((response_headers, response_code, response_body))


@APP.route('/connected-systems/')
async def csa_catalog_root():
    request.collection = None
    """
    Connected Systems API root endpoint

    :returns: HTTP response
    """
    return await get_response(await csapi_.get_connected_systems_root(request))


@APP.route('/systems', methods=['GET', 'POST'])
@APP.route('/systems/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def systems_path(path=None):
    request.collection = "systems"
    """
    Connect Systems API path endpoint

    :param path: path

    :returns: HTTP response
    """
    if request.method == 'GET':
        if path is not None:
            return await get_response(await csapi_.get_systems(request, ("id", path)))
        else:
            return await get_response(await csapi_.get_systems(request, None))
    elif request.method == 'PUT':
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        if request.content_type is not None:
            if request.content_type == 'application/sml+json':
                return await get_response(await csapi_.post_systems(request))
            else:
                return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


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
                return await get_response(await csapi_.get_systems(request, ("parent", path)))
            case "deployments":
                return await get_response(await csapi_.get_deployments(request, ("system", path)))
            case "samplingFeatures":
                return await get_response(await csapi_.get_sampling_features(request, ("system", path)))
            case "datastreams":
                return await get_response(await csapi_.get_datastreams(request, ("system", path)))
    elif request.method == 'POST':
        match collection:
            case "subsystems":
                return await get_response(await csapi_.post_systems(request, ("parent", path)))
            case "samplingFeatures":
                return await get_response(await csapi_.post_sampling_feature(request, ("system", path)))
            case "datastreams":
                return await get_response(await csapi_.post_datastreams(request, ("system", path)))


@APP.route('/procedures', methods=['GET', 'POST'])
@APP.route('/procedures/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def procedures_path(path=None):
    request.collection = "procedures"
    if request.method == 'GET':
        if path is not None:
            return await get_response(await csapi_.get_procedures(request, ("id", path)))
        else:
            return await get_response(await csapi_.get_procedures(request, None))
    elif request.method == 'POST':
        if request.content_type is not None:
            if request.content_type == 'application/sml+json':
                return await get_response(await csapi_.post_procedures(request))
            else:
                return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/deployments', methods=['GET', 'POST'])
@APP.route('/deployments/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def deployments_path(path=None):
    request.collection = "deployments"
    if request.method == 'GET':
        if path is not None:
            return await get_response(await csapi_.get_deployments(request, ("id", path)))
        else:
            return await get_response(await csapi_.get_deployments(request, None))
    elif request.method == 'POST':
        if request.content_type is not None:
            if request.content_type == 'application/sml+json':
                return await get_response(await csapi_.post_deployments(request))
            else:
                return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


# @APP.route('/deployments/<path:path>/systems', methods=['GET', 'POST'])
# async def deployments_subpath(path):
#     request.collection = "systems"
#     if request.method == 'GET':
#         return await get_response(await csapi_.get_systems(request, ("system", path)))
#     elif request.method == 'POST':
#         if request.content_type is not None:
#             if request.content_type == 'application/json':
#                 return await get_response(await csapi_.post_systems_to_deployment(request, ("deplyoment", path)))
#             else:
#                 return await get_response((None, HTTPStatus.BAD_REQUEST, ""))



@APP.route('/samplingFeatures', methods=['GET'])
@APP.route('/samplingFeatures/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def properties_path(path=None):
    request.collection = "samplingFeatures"
    if request.method == 'GET':
        if path is not None:
            return await get_response(await csapi_.get_sampling_features(request, ("id", path)))
        else:
            return await get_response(await csapi_.get_sampling_features(request, None))
    else:
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/properties', methods=['GET', 'POST'])
@APP.route('/properties/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def properties_subpath(path=None):
    request.collection = "properties"
    if request.method == 'GET':
        if path is not None:
            return await get_response(await csapi_.get_properties(request, ("id", path)))
        else:
            return await get_response(await csapi_.get_properties(request, None))
    elif request.method == 'POST':
        return await get_response(await csapi_.post_properties(request, None))
    else:
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/datastreams', methods=['GET'])
@APP.route('/datastreams/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def datastreams_path(path=None):
    request.collection = "datastreams"
    if request.method == 'GET':
        if path is not None:
            return await get_response(await csapi_.get_datastreams(request, ("id", path)))
        else:
            return await get_response(await csapi_.get_datastreams(request, None))
    else:
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/datastreams/<path:path>/schema', methods=['GET', 'PUT'])
@APP.route('/datastreams/<path:path>/observations', methods=['GET', 'POST'])
async def datastreams_subpath(path=None):
    property = request.path.split('/')[-1]
    request.collection = property
    if request.method == 'GET':
        if property == "schema":
            return await get_response(await csapi_.get_datastreams_schema(request, ("id", path)))
        elif property == "observations":
            return await get_response(await csapi_.get_observations(request, ("datastream", path)))
    elif request.method == 'PUT':
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    elif request.method == 'POST':
        return await get_response(await csapi_.post_observations(request, ("datastream", path)))


@APP.route('/observations', methods=['GET'])
@APP.route('/observations/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def observations_path(path=None):
    request.collection = "observations"
    if request.method == 'GET':
        if path is not None:
            return await get_response(await csapi_.get_observations(request, ("id", path)))
        else:
            return await get_response(await csapi_.get_observations(request, None))
    else:
        return await get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


async def get_response(result: tuple):
    """
    Creates a Quart Response object and updates matching headers.

    :param result: The result of the API call.
                   This should be a tuple of (headers, status, content).

    :returns: A Response instance.
    """

    headers, status, content = result
    response = await make_response(content, status)

    if headers:
        response.headers = headers
    return response


@APP.before_serving
async def init_db():
    """ Initialize peristent database/provider connections """
    if csapi_.csa_provider_part1:
        await csapi_.csa_provider_part1.open()
    if csapi_.csa_provider_part2:
        await csapi_.csa_provider_part2.open()


@APP.after_serving
async def close_db():
    """ Clean exit database/provider connections """
    if csapi_.csa_provider_part1:
        await csapi_.csa_provider_part1.close()
    if csapi_.csa_provider_part2:
        await csapi_.csa_provider_part2.close()


def run():
    ## Only used in local development - gunicorn is used for production
    os.environ["PYGEOAPI_CONFIG"] = "pygeoapi-config.yml"
    os.environ["PYGEOAPI_OPENAPI"] = "openapi-config-csa.yml"
    APP.run(debug=True,
            host=api_.config['server']['bind']['host'],
            port=api_.config['server']['bind']['port'])


if __name__ == "__main__":
    run()
