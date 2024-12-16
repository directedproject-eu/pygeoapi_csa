from quart import Blueprint, request

from pygeoapi.flask_app import api_
from util import CompatibilityRequest, to_response

oapip = Blueprint('oapip', __name__)


@oapip.route('/processes')
@oapip.route('/processes/<process_id>')
async def get_processes(process_id=None):
    """
    OGC API - Processes description endpoint

    :param process_id: process identifier

    :returns: HTTP response
    """
    compat = CompatibilityRequest(None, request.headers, request.args)
    return await to_response(api_.describe_processes(compat, process_id))


@oapip.route('/jobs')
@oapip.route('/jobs/<job_id>', methods=['GET', 'DELETE'])
async def get_jobs(job_id=None):
    """
    OGC API - Processes jobs endpoint

    :param job_id: job identifier

    :returns: HTTP response
    """
    compat = CompatibilityRequest(None, request.headers, request.args)

    if job_id is None:
        return await to_response(api_.get_jobs(compat))
    else:
        if request.method == 'DELETE':  # dismiss job
            return await to_response(api_.delete_job(compat, job_id))
        else:  # Return status of a specific job
            return await to_response(api_.get_jobs(compat, job_id))


@oapip.route('/processes/<process_id>/execution', methods=['POST'])
async def execute_process_jobs(process_id):
    """
    OGC API - Processes execution endpoint

    :param process_id: process identifier

    :returns: HTTP response
    """
    compat = CompatibilityRequest(await request.body, request.headers, request.args)

    return await to_response(api_.execute_process(compat, process_id))


@oapip.route('/jobs/<job_id>/results',
             methods=['GET'])
async def get_job_result(job_id=None):
    """
    OGC API - Processes job result endpoint

    :param job_id: job identifier

    :returns: HTTP response
    """
    compat = CompatibilityRequest(None, request.headers, request.args)
    return await to_response(api_.get_job_result(compat, job_id))


@oapip.route('/jobs/<job_id>/results/<resource>', methods=['GET'])
async def get_job_result_resource(job_id, resource):
    """
    OGC API - Processes job result resource endpoint

    :param job_id: job identifier
    :param resource: job resource

    :returns: HTTP response
    """
    compat = CompatibilityRequest(None, request.headers, request.args)
    return await to_response(api_.get_job_result_resource(
        compat, job_id, resource))
