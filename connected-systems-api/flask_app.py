import os
from functools import wraps

from flask import Flask, request
from pygeoapi import flask_app
from pygeoapi.flask_app import STATIC_FOLDER, API_RULES, CONFIG, api_, OPENAPI
from cs_api import *

APP = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='/static')
APP.url_map.strict_slashes = API_RULES.strict_slashes
APP.config['JSONIFY_PRETTYPRINT_REGULAR'] = CONFIG['server'].get('pretty_print', True)

PLUGINS["provider"]["toardb"] = "provider.toardb_csa.ToarDBProvider"
PLUGINS["provider"]["ElasticSearchConnectedSystems"] = "provider.elasticsearch_csa.ConnectedSystemsESProvider"

csapi_ = CSAPI(CONFIG, OPENAPI)

from werkzeug.datastructures import MultiDict


def mutable_args(func):
    @wraps(func)
    def f(*args, **kwargs):
        http_args = request.args.to_dict()
        request.args = MultiDict(http_args)
        return func(*args, **kwargs)

    return f


@APP.route('/')
def landing_page():
    return flask_app.get_response(csapi_.landing_page(request))


@APP.route('/openapi')
def openapi():
    return flask_app.openapi()


@APP.route('/conformance')
def conformance():
    return flask_app.get_response(csapi_.conformance(request))


@APP.route('/collections')
@APP.route('/collections/<path:collection_id>')
@mutable_args
def collections(collection_id: str = None):
    """
    OGC API collections endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    # Overwrite original request format with json so we can parse response in wrapper
    original_format = None
    if "f" in request.args:
        original_format = request.args["f"]
    request.args["f"] = "json"
    response = csapi_.get_collections(request,
                                      api_.describe_collections(request, collection_id),
                                      original_format,
                                      collection_id)
    return flask_app.get_response(response)


@APP.route('/collections/<path:collection_id>/items')
@APP.route('/collections/<path:collection_id>/items/<path:item_id>')
def collection_items(collection_id: str, item_id: str = None):
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
    return flask_app.get_response((response_headers, response_code, response_body))


@APP.route('/connected-systems/')
def csa_catalog_root():
    """
    Connected Systems API root endpoint

    :returns: HTTP response
    """
    return flask_app.get_response(csapi_.get_connected_systems_root(request))


@APP.route('/systems', methods=['GET', 'POST'])
@APP.route('/systems/<path:path>', methods=['GET', 'PUT', 'DELETE'])
def systems_path(path=None):
    """
    Connect Systems API path endpoint

    :param path: path

    :returns: HTTP response
    """
    if request.method == 'GET':
        if path is not None:
            return flask_app.get_response(csapi_.get_systems(request, ("id", path)))
        else:
            return flask_app.get_response(csapi_.get_systems(request, None))
    elif request.method == 'PUT':
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        if request.content_type is not None:
            if request.content_type == 'application/sml+json':
                return flask_app.get_response(
                    csapi_.post_systems(request))
            else:
                return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/systems/<path:path>/members', methods=['GET', 'POST'])
@APP.route('/systems/<path:path>/deployments', methods=['GET'])
@APP.route('/systems/<path:path>/samplingFeatures', methods=['GET', 'POST'])
@APP.route('/systems/<path:path>/datastreams', methods=['GET'])
def systems_subpath(path=None):
    property = request.path.split('/')[-1]
    if request.method == 'GET':
        if property == "members":
            return flask_app.get_response(csapi_.get_systems(request, ("parent", path)))
        elif property == "deployments":
            return flask_app.get_response(csapi_.get_deployments(request, ("system", path)))
        elif property == "samplingFeatures":
            return flask_app.get_response(csapi_.get_sampling_features(request, ("system", path)))
        elif property == "datastreams":
            return flask_app.get_response(csapi_.get_datastreams(request, ("system", path)))
    elif request.method == 'POST':
        if property == "members":
            return flask_app.get_response(csapi_.post_systems(request, ("parent", path)))
        elif property == "samplingFeatures":
            return flask_app.get_response(csapi_.post_sampling_feature(request, ("system", path)))


@APP.route('/procedures', methods=['GET', 'POST'])
@APP.route('/procedures/<path:path>', methods=['GET', 'PUT', 'DELETE'])
def procedures_path(path=None):
    if request.method == 'GET':
        if path is not None:
            return flask_app.get_response(csapi_.get_procedures(request, ("id", path)))
        else:
            return flask_app.get_response(csapi_.get_procedures(request, None))
    elif request.method == 'POST':
        if request.content_type is not None:
            if request.content_type == 'application/sml+json':
                return flask_app.get_response(
                    csapi_.post_procedures(request))
            else:
                return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/deployments', methods=['GET', 'POST'])
@APP.route('/deployments/<path:path>', methods=['GET', 'PUT', 'DELETE'])
def deployments_path(path=None):
    if request.method == 'GET':
        if path is not None:
            return flask_app.get_response(csapi_.get_deployments(request, ("id", path)))
        else:
            return flask_app.get_response(csapi_.get_deployments(request, None))
    elif request.method == 'POST':
        if request.content_type is not None:
            if request.content_type == 'application/sml+json':
                return flask_app.get_response(
                    csapi_.post_deployments(request))
            else:
                return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/deployments/<path:path>/systems', methods=['GET', 'POST'])
def deployments_subpath(path):
    if request.method == 'GET':
        return flask_app.get_response(csapi_.get_systems(request, ("system", path)))
    elif request.method == 'POST':
        if request.content_type is not None:
            if request.content_type == 'application/json':
                return flask_app.get_response(
                    csapi_.post_systems_to_deployment(request, ("deplyoment", path)))
            else:
                return flask_app.get_response((None, HTTPStatus.BAD_REQUEST, ""))


@APP.route('/samplingFeatures', methods=['GET'])
@APP.route('/samplingFeatures/<path:path>', methods=['GET', 'PUT', 'DELETE'])
def properties_path(path=None):
    if request.method == 'GET':
        if path is not None:
            return flask_app.get_response(csapi_.get_sampling_features(request, ("id", path)))
        else:
            return flask_app.get_response(csapi_.get_sampling_features(request, None))
    else:
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/properties', methods=['GET', 'POST'])
@APP.route('/properties/<path:path>', methods=['GET', 'PUT', 'DELETE'])
def properties_subpath(path=None):
    if request.method == 'GET':
        if path is not None:
            return flask_app.get_response(csapi_.get_properties(request, ("id", path)))
        else:
            return flask_app.get_response(csapi_.get_properties(request, None))
    elif request.method == 'POST':
        return flask_app.get_response(csapi_.post_properties(request, None))
    else:
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/datastreams', methods=['GET', 'POST'])
@APP.route('/datastreams/<path:path>', methods=['GET', 'PUT', 'DELETE'])
def datastreams_path(path=None):
    if request.method == 'GET':
        if path is not None:
            return flask_app.get_response(csapi_.get_datastreams(request, ("id", path)))
        else:
            return flask_app.get_response(csapi_.get_datastreams(request, None))
    elif request.method == 'POST':
        if request.content_type is not None:
            if request.content_type == 'application/sml+json':
                return flask_app.get_response(
                    csapi_.post_datastreams(request))
            else:
                return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/datastreams/<path:path>/schema', methods=['GET', 'PUT'])
@APP.route('/datastreams/<path:path>/observations', methods=['GET', 'POST'])
def datastreams_subpath(path=None):
    property = request.path.split('/')[-1]
    if request.method == 'GET':
        if property == "schema":
            return flask_app.get_response(csapi_.get_datastreams_schema(request, ("id", path)))
        elif property == "observations":
            return flask_app.get_response(csapi_.get_observations(request, ("datastream", path)))
    elif request.method == 'PUT':
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    elif request.method == 'POST':
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


@APP.route('/observations', methods=['GET'])
@APP.route('/observations/<path:path>', methods=['GET', 'PUT', 'DELETE'])
def observations_path(path=None):
    if request.method == 'GET':
        if path is not None:
            return flask_app.get_response(csapi_.get_observations(request, ("id", path)))
        else:
            return flask_app.get_response(csapi_.get_observations(request, None))
    elif request.method == 'POST':
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))
    else:
        return flask_app.get_response((None, HTTPStatus.NOT_IMPLEMENTED, ""))


def run():
    ## Only used in local development - gunicorn is used for production
    # os.environ["PYGEOAPI_CONFIG"] = "pygeoapi-config.yml"
    APP.run(debug=True,
            host=api_.config['server']['bind']['host'],
            port=api_.config['server']['bind']['port'])


if __name__ == "__main__":
    run()
