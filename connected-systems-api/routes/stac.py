from quart import Blueprint, request

from pygeoapi.flask_app import api_
from util import *

stac = Blueprint('stac', __name__)


@stac.route('/stac')
async def stac_catalog_root():
    """
    STAC root endpoint

    :returns: HTTP response
    """
    compat = CompatibilityRequest(None, request.headers, request.args)
    return await to_response(api_.get_stac_root(compat))


@stac.route('/stac/<path:path>')
async def stac_catalog_path(path):
    """
    STAC path endpoint

    :param path: path

    :returns: HTTP response
    """
    compat = CompatibilityRequest(None, request.headers, request.args)
    return await to_response(api_.get_stac_path(compat, path))
