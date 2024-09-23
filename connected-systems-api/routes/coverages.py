from quart import request, Blueprint

from pygeoapi.flask_app import api_
from util import *
coverage = Blueprint('coverage', __name__)


@coverage.route('/collections/<path:collection_id>/coverage')
async def collection_coverage(collection_id):
    """
    OGC API - Coverages coverage endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """
    compat = CompatibilityRequest(None, request.headers, request.args)
    return await to_response(api_.get_collection_coverage(compat, collection_id))
