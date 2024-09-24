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
import os
import pathlib
from http import HTTPMethod

import jsonschema
from jsonschema import validate
from pygeoapi.api import *
from pygeoapi.flask_app import CONFIG, OPENAPI
from pygeoapi.provider.base import ProviderItemNotFoundError
from pygeoapi.util import render_j2_template, to_json

from meta import CSMeta
from provider.definitions import *
from util import *


package_dir = pathlib.Path(__file__).parent


class CSAPI(CSMeta):
    """
        API Object implementing OGC API Connected Systems
    """
    csa_schemas = {}
    strict_validation = True

    def __init__(self, config, openapi):
        super().__init__(config, openapi)

        if config['dynamic-resources'] is not None:
            api_part1 = config['dynamic-resources'].get('connected-systems-api-part1', None)
            if api_part1 is not None:
                provider_definition = api_part1['provider']
                provider_definition["base_url"] = self.base_url
                self.provider_part1 = load_plugin('provider', provider_definition)

                if api_part1.get("strict_validation", None) is not None:
                    self.strict_validation = bool(api_part1["strict_validation"])

                if self.config.get('resources') is None:
                    self.config['resources'] = {}

                # TODO: refresh this upon modification of the datastore (e.g. adding new collections)
                for name, location in [(EntityType.SYSTEMS, "schemas/connected-systems/system.schema"),
                                       (EntityType.PROCEDURES, "schemas/connected-systems/procedure.schema"),
                                       (EntityType.PROPERTIES, "schemas/connected-systems/property.schema"),
                                       (EntityType.SAMPLING_FEATURES,
                                        "schemas/connected-systems/samplingFeature.schema"),
                                       (EntityType.DEPLOYMENTS, "schemas/connected-systems/deployment.schema")]:
                    with open(os.path.join(package_dir, location), 'r') as definition:
                        self.csa_schemas[name] = json.load(definition)
            api_part2 = config['dynamic-resources'].get('connected-systems-api-part2', None)

            if api_part2 is not None:
                provider_definition = api_part2['provider']
                provider_definition["base_url"] = self.base_url
                self.provider_part2 = load_plugin('provider', provider_definition)

                if self.config.get('resources') is None:
                    self.config['resources'] = {}

                for name, location in [
                    (EntityType.DATASTREAMS, "schemas/connected-systems/datastream.schema"),
                    (EntityType.OBSERVATIONS, "schemas/connected-systems/observation.schema")]:
                    with open(os.path.join(package_dir, location), 'r') as definition:
                        self.csa_schemas[name] = json.load(definition)

    @parse_request
    @jsonldify
    async def get_collections(self,
                              request: AsyncAPIRequest,
                              template: Tuple[dict, int, str],
                              original_format: str,
                              collection_id: str = None) -> APIResponse:
        """
        Adds Connected-Systems collections to existing response
        """
        # Start new response object if resources is empty, else reuse existing object
        fcm = None
        if template[1] == HTTPStatus.NOT_FOUND:
            fcm = {"collections": [], "links": []}
        else:
            fcm = json.loads(template[2])

        # query collections
        data = None
        try:
            if collection_id is not None:
                request.params['id'] = collection_id

            parameters = parse_query_parameters(CollectionParams(), request.params,
                                                self.base_url + "/" + request.path_info)
            parameters.format = original_format
            data = await self.provider_part1.query_collections(parameters)
        except ProviderItemNotFoundError:
            # element was not found in resources nor dynamic-resources, return 404
            if template[1] == HTTPStatus.NOT_FOUND:
                return template
            # else: there is already content

        headers = template[0]
        if data:
            if collection_id is not None:
                headers["Content-Type"] = "application/json"
                return headers, HTTPStatus.OK, to_json(data[0][0], self.pretty_print)
            else:
                fcm['collections'].extend(data[0])
                if original_format == F_HTML:  # render
                    fcm['collections_path'] = f"{self.base_url}/collections"
                    headers["Content-Type"] = "text/html"
                    content = render_j2_template(self.tpl_config,
                                                 'collections/index.html',
                                                 fcm,
                                                 request.locale)
                    return headers, HTTPStatus.OK, content
                else:
                    headers["Content-Type"] = "application/json"
                    return headers, HTTPStatus.OK, to_json(fcm, self.pretty_print)

        return headers, HTTPStatus.OK, to_json(fcm, self.pretty_print)

    @parse_request
    async def get_collection_items(self, request: AsyncAPIRequest, collection_id: str, item_id: str) -> APIResponse:
        headers = request.get_response_headers(**self.api_headers)
        try:
            request_params = request.params
            if item_id:
                request_params['id'] = item_id

            parameters = parse_query_parameters(CollectionParams(), request_params, self.base_url + "/" +
                                                request.path_info)
            data = await self.provider_part1.query_collection_items(collection_id, parameters)
            return self._format_json_response(request, headers, data, item_id is None)
        except ProviderItemNotFoundError:
            return headers, HTTPStatus.NOT_FOUND, ""

    @parse_request
    async def get(self,
                  request: AsyncAPIRequest,
                  collection: EntityType,
                  path: Path = None
                  ) -> APIResponse:
        """
        Provide Connected Systems API Collections

        :param request: APIRequest instance with query params
        :param collection: Collection to query
        :param path: Additional information extracted from path

        :returns: tuple of headers, status code, content
        """

        match collection:
            case EntityType.SYSTEMS:
                handler = self.provider_part1.query_systems
                params = SystemsParams()
                allowed_mimetypes = [ALLOWED_MIMES.F_HTML, ALLOWED_MIMES.F_SMLJSON, ALLOWED_MIMES.F_GEOJSON]
            case EntityType.DEPLOYMENTS:
                handler = self.provider_part1.query_deployments
                params = DeploymentsParams()
                allowed_mimetypes = [ALLOWED_MIMES.F_HTML, ALLOWED_MIMES.F_SMLJSON, ALLOWED_MIMES.F_GEOJSON]
            case EntityType.PROCEDURES:
                handler = self.provider_part1.query_procedures
                params = ProceduresParams()
                allowed_mimetypes = [ALLOWED_MIMES.F_HTML, ALLOWED_MIMES.F_SMLJSON, ALLOWED_MIMES.F_GEOJSON]
            case EntityType.SAMPLING_FEATURES:
                handler = self.provider_part1.query_sampling_features
                params = SamplingFeaturesParams()
                allowed_mimetypes = [ALLOWED_MIMES.F_HTML, ALLOWED_MIMES.F_GEOJSON]
            case EntityType.PROPERTIES:
                handler = self.provider_part1.query_properties
                params = CSAParams()
                allowed_mimetypes = [ALLOWED_MIMES.F_HTML, ALLOWED_MIMES.F_SMLJSON]
            case EntityType.DATASTREAMS:
                handler = self.provider_part2.query_datastreams
                params = DatastreamsParams()
                allowed_mimetypes = [ALLOWED_MIMES.F_HTML, ALLOWED_MIMES.F_JSON]
            case EntityType.DATASTREAMS_SCHEMA:
                handler = self.provider_part2.query_datastreams
                params = DatastreamsParams()
                params.schema = True
                allowed_mimetypes = [ALLOWED_MIMES.F_JSON]
            case EntityType.OBSERVATIONS:
                handler = self.provider_part2.query_observations
                params = ObservationsParams()
                allowed_mimetypes = [ALLOWED_MIMES.F_HTML, ALLOWED_MIMES.F_JSON]

        if allowed_mimetypes and not request.is_valid(allowed_mimetypes):
            # Check if mime_type is allowed
            return self.get_exception(
                HTTPStatus.BAD_REQUEST,
                {},
                request.format,
                'InvalidMimetype',
                f"invalid mimetype supplied! expected {[f.value for f in allowed_mimetypes]} got '{request.format}'")

        headers = request.get_response_headers(**self.api_headers, force_type=request.format)
        collection = True
        # Expand parameters with additional information based on path
        if path is not None:
            # Check that id is not malformed.
            if not re.match("^[\\w-]+$", path[1]):
                return self.get_exception(
                    HTTPStatus.BAD_REQUEST,
                    headers,
                    request.format,
                    'InvalidParameterValue',
                    "entity identifer is malformed!")

            # TODO: does the case exist where a property is specified both
            #  in url and query params and we overwrite stuff here?
            request.params[path[0]] = path[1]

            if path[0] == "id":
                collection = False

        if request.format == ALLOWED_MIMES.F_HTML.value:
            return self._format_html_response(request, headers, collection)

        try:
            parameters = parse_query_parameters(params, request.params, self.base_url + "/" + request.path_info)
            parameters.format = request.format
            data = await handler(parameters)

            return self._format_json_response(request, headers, data, collection)
        except ProviderItemNotFoundError:
            return self.get_exception(
                HTTPStatus.NOT_FOUND,
                headers,
                request.format,
                'NotFound',
                "entity not found")
        except ProviderInvalidQueryError as err:
            return self.get_exception(
                HTTPStatus.BAD_REQUEST,
                headers,
                request.format,
                'BadRequest',
                "bad request: " + err.message)

    @parse_request
    async def put(self, request: AsyncAPIRequest, collection: EntityType, path: Path = None):
        return await self._upsert(request, HTTPMethod.PUT, collection, path, self.strict_validation)

    @parse_request
    async def post(self, request: AsyncAPIRequest, collection: EntityType, path: Path = None):
        return await self._upsert(request, HTTPMethod.POST, collection, path, self.strict_validation)

    @parse_request
    async def patch(self, request: AsyncAPIRequest, collection: EntityType, path: Path = None):
        LOGGER.warn("TODO: add validation for properties")
        return await self._upsert(request, HTTPMethod.PATCH, collection, path, False)

    @parse_request
    async def delete(self,
                     request: AsyncAPIRequest,
                     collection: EntityType,
                     path: Path = None) -> APIResponse:
        # multiplex provider implementations (part 1 or part 2)
        if (collection == EntityType.OBSERVATIONS
                or collection == EntityType.DATASTREAMS
                or collection == EntityType.DATASTREAMS_SCHEMA):
            provider = self.provider_part2
        else:
            provider = self.provider_part1

        try:
            await provider.delete(collection, path[1], request.params.get("cascade", False))
            return [], HTTPStatus.OK, ""
        except ProviderItemNotFoundError:
            return self.get_exception(
                HTTPStatus.NOT_FOUND,
                request.get_response_headers(**self.api_headers),
                request.format,
                'NotFound',
                "entity not found")
        except ProviderInvalidQueryError as err:
            return self.get_exception(
                HTTPStatus.BAD_REQUEST,
                request.get_response_headers(**self.api_headers),
                request.format,
                'BadRequest',
                "bad request: " + err.message)

    async def _upsert(self,
                      request: AsyncAPIRequest,
                      method: HTTPMethod,
                      collection: EntityType,
                      path: Path = None,
                      shall_validate: bool = True
                      ) -> APIResponse:

        # multiplex provider implementations (part 1 or part 2)
        if (collection == EntityType.OBSERVATIONS
                or collection == EntityType.DATASTREAMS
                or collection == EntityType.DATASTREAMS_SCHEMA):
            provider = self.provider_part2
        else:
            provider = self.provider_part1

        headers = request.get_response_headers(**self.api_headers)
        entity = json.loads(request.data)

        # Validate against json schema if required
        # may be turned off for increased performance
        if shall_validate:
            schema = self.csa_schemas[collection]
            try:
                validate(instance=entity, schema=schema)

            except jsonschema.exceptions.ValidationError as ex:
                return self.get_exception(
                    HTTPStatus.BAD_REQUEST,
                    headers,
                    request.format,
                    'InvalidParameterValue',
                    ex.message)
        # remove additional fields that cannot be set using POST/PUT but only through Path but pass validation
        if "parent" in entity:
            return self.get_exception(
                HTTPStatus.BAD_REQUEST,
                headers,
                request.format,
                'InvalidParameterValue',
                "Cannot set parent through request body!")

        if path is not None:
            entity[path[0]] = path[1]

        try:
            # passthru to provider
            match method:
                case HTTPMethod.POST:
                    response = provider.create(collection, entity)
                case HTTPMethod.PUT:
                    response = provider.replace(collection, path[1], entity)
                case HTTPMethod.PATCH:
                    response = provider.update(collection, path[1], entity)
                case _:
                    raise Exception(f"unrecognized HTTMethod {method}")

            return headers, HTTPStatus.NO_CONTENT, to_json(await response, self.pretty_print)
        except Exception as ex:
            return self.get_exception(
                HTTPStatus.BAD_REQUEST,
                headers,
                request.format,
                'InvalidParameterValue',
                ex.args)

    def _format_html_response(self, request, headers, is_collection: bool):
        if is_collection:
            collection = request.collection
            data = {
                "config": {
                    "collection": collection,
                    "backend-url": self.base_url + "/",
                },
                "breadcrumbs": [
                    (collection, "/")
                ]
            }
            if collection in ["subsystems", "datastreams", "deployments"]:
                subcollection = None
                if request.params.get("system"):
                    subcollection = ("system", request.params.get("system"))
                if request.params.get("parent"):
                    subcollection = ("parent", request.params.get("parent"))

                if subcollection:
                    data["config"][subcollection[0]] = subcollection[1]
                    data["breadcrumbs"] = [
                        ("systems", "../"),
                        (subcollection[1], f"../{subcollection[1]}"),
                        (collection, collection)
                    ]
        else:
            collection, id = request.path_info.split("/")
            data = {
                "config": {
                    "collection": collection,
                    "backend-url": self.base_url + "/",
                    "id": id
                },
                "breadcrumbs": [
                    (request.collection, "."),
                    (id, id)
                ]
            }

        data["links"] = [
            {
                "rel": "alternate",
                "type": "application/json",
                "href": "?f=application/json"
            }
        ]
        content = render_j2_template(self.tpl_config,
                                     'templates/connected-systems/viewer.html',
                                     data,
                                     request.locale)
        return headers, HTTPStatus.OK, content

    def _format_json_response(self, request, headers, data, is_collection: bool) -> APIResponse:
        if data is None:
            return headers, HTTPStatus.NOT_FOUND, ""
        match request.format:
            case ALLOWED_MIMES.F_GEOJSON.value:
                response = {
                    "type": "FeatureCollection",
                    "features": [item for item in data[0]],
                    "links": [link for link in data[1]],
                } if is_collection else data[0][0]
                return headers, HTTPStatus.OK, to_json(response, self.pretty_print)
            case _:
                response = {
                    "items": [item for item in data[0]],
                    "links": [link for link in data[1]],
                } if is_collection else data[0][0]

                return headers, HTTPStatus.OK, to_json(response, self.pretty_print)


PLUGINS["provider"]["toardb"] = "provider.toardb_csa.ToarDBProvider"
PLUGINS["provider"]["ElasticSearchConnectedSystems"] = \
    "provider.part1.elasticsearch.ConnectedSystemsESProvider"
PLUGINS["provider"]["TimescaleDBConnectedSystems"] = \
    "provider.part2.timescaledb.ConnectedSystemsTimescaleDBProvider"

csapi_ = CSAPI(CONFIG, OPENAPI)
