from quart import request, Blueprint

from pygeoapi.flask_app import api_
from util import *

edr = Blueprint('edr', __name__)


@edr.route('/collections/<path:collection_id>/position')
@edr.route('/collections/<path:collection_id>/area')
@edr.route('/collections/<path:collection_id>/cube')
@edr.route('/collections/<path:collection_id>/radius')
@edr.route('/collections/<path:collection_id>/trajectory')
@edr.route('/collections/<path:collection_id>/corridor')
@edr.route('/collections/<path:collection_id>/locations/<location_id>')  # noqa
@edr.route('/collections/<path:collection_id>/locations')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/position')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/area')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/cube')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/radius')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/trajectory')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/corridor')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/locations/<location_id>')  # noqa
@edr.route('/collections/<path:collection_id>/instances/<instance_id>/locations')  # noqa
async def get_collection_edr_query(collection_id, instance_id=None, location_id=None):
    """
    OGC EDR API endpoints

    :param collection_id: collection identifier
    :param instance_id: instance identifier
    :param location_id: location id of a /locations/<location_id> query

    :returns: HTTP response
    """
    compat = CompatibilityRequestf(None, request.headers, request.args)
    if location_id:
        query_type = 'locations'
    else:
        query_type = request.path.split('/')[-1]

    return await to_response(api_.get_collection_edr_query(compat, collection_id,
                                                           instance_id, query_type,
                                                           location_id))
