from quart import request, Blueprint

from pygeoapi.flask_app import api_, CONFIG, OPENAPI
from util import *
from provider.definitions import *
from api import csapi_

csa = Blueprint('csa', __name__)


@csa.route('/connected-systems/')
async def csa_catalog_root():
    request.collection = None
    """
    Connected Systems API root endpoint

    :returns: HTTP response
    """
    return await to_response(await csapi_.overview(request))


@csa.route('/systems', methods=['GET', 'POST'])
@csa.route('/systems/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def systems_path(path=None):
    request.collection = "systems"
    return await _default_handler(path, EntityType.SYSTEMS)


@csa.route('/systems/<path:path>/subsystems', methods=['GET', 'POST'])
@csa.route('/systems/<path:path>/deployments', methods=['GET'])
@csa.route('/systems/<path:path>/samplingFeatures', methods=['GET', 'POST'])
@csa.route('/systems/<path:path>/datastreams', methods=['GET', 'POST'])
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


@csa.route('/procedures', methods=['GET', 'POST'])
@csa.route('/procedures/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def procedures_path(path=None):
    request.collection = "procedures"
    return await _default_handler(path, EntityType.PROCEDURES)


@csa.route('/deployments', methods=['GET', 'POST'])
@csa.route('/deployments/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def deployments_path(path=None):
    request.collection = "deployments"
    return await _default_handler(path, EntityType.DEPLOYMENTS)


@csa.route('/samplingFeatures', methods=['GET'])
@csa.route('/samplingFeatures/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def properties_path(path=None):
    request.collection = "samplingFeatures"
    return await _default_handler(path, EntityType.SAMPLING_FEATURES)


@csa.route('/properties', methods=['GET', 'POST'])
@csa.route('/properties/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
async def properties_subpath(path=None):
    request.collection = "properties"
    return await _default_handler(path, EntityType.PROPERTIES)


@csa.route('/datastreams', methods=['GET'])
@csa.route('/datastreams/<path:path>', methods=['GET', 'PATCH', 'PUT', 'DELETE'])
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


@csa.route('/datastreams/<path:path>/schema', methods=['GET', 'PUT'])
async def datastreams_schema(path=None):
    request.collection = "schema"
    if request.method == 'GET':
        return await to_response(await csapi_.get(request, EntityType.DATASTREAMS_SCHEMA, ("id", path)))
    else:
        return await to_response(await csapi_.put(request, EntityType.DATASTREAMS_SCHEMA, ("id", path)))


@csa.route('/datastreams/<path:path>/observations', methods=['GET', 'POST'])
async def datastreams_observations(path=None):
    request.collection = "observations"
    if request.method == 'GET':
        return await to_response(await csapi_.get(request, EntityType.OBSERVATIONS, ("datastream", path)))
    else:
        return await to_response(await csapi_.post(request, EntityType.OBSERVATIONS, ("datastream", path)))


@csa.route('/observations', methods=['GET'])
@csa.route('/observations/<path:path>', methods=['GET', 'PUT', 'DELETE'])
async def observations_path(path=None):
    request.collection = "observations"
    return await _default_handler(path, EntityType.OBSERVATIONS)
