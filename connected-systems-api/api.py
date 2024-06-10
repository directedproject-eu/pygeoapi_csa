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
from typing import Callable, Self

import jsonschema
from pygeoapi.api import *
from pygeoapi.provider.base import ProviderItemNotFoundError

from pygeoapi.util import filter_dict_by_key_value, render_j2_template, to_json
from provider.definitions import *

from jsonschema import validate


class AsyncAPIRequest(APIRequest):
    @classmethod
    async def with_data(cls, request, supported_locales) -> Self:
        api_req = cls(request, supported_locales)
        api_req._data = await request.data
        return api_req


def process(func):
    """
    Decorator that transforms an incoming Request instance specific to the
    web framework into a generic :class:`AsyncAPIRequest` instance.

    :param func: decorated function

    :returns: `func`
    """

    async def inner(*args):
        cls, req_in = args[:2]
        req_out = await AsyncAPIRequest.with_data(req_in, getattr(cls, 'locales', set()))
        if len(args) > 2:
            return await func(cls, req_out, *args[2:])
        else:
            return await func(cls, req_out)

    return inner


class CSAPI(API):
    csa_provider_part1: ConnectedSystemsPart1Provider | None
    csa_provider_part2: ConnectedSystemsPart2Provider | None

    csa_schemas = {}

    def __init__(self, config, openapi):
        super().__init__(config, openapi)

        if config['dynamic-resources'] is not None:
            api_part1 = config['dynamic-resources'].get('connected-systems-api-part1', None)
            if api_part1 is not None:
                provider_definition = api_part1['provider']
                provider_definition["base_url"] = self.base_url
                self.csa_provider_part1 = load_plugin('provider', provider_definition)

                if self.config.get('resources') is None:
                    self.config['resources'] = {}

                # TODO: refresh this upon modification of the datastore (e.g. adding new collections)
                for name, location in [("system", "schemas/connected-systems/system.schema"),
                                       ("procedure", "schemas/connected-systems/procedure.schema"),
                                       ("property", "schemas/connected-systems/property.schema"),
                                       ("samplingFeature", "schemas/connected-systems/samplingFeature.schema"),
                                       ("deployment", "schemas/connected-systems/deployment.schema")]:
                    with open(location, 'r') as definition:
                        self.csa_schemas[name] = json.load(definition)
            api_part2 = config['dynamic-resources'].get('connected-systems-api-part2', None)

            if api_part2 is not None:
                provider_definition = api_part2['provider']
                provider_definition["base_url"] = self.base_url
                self.csa_provider_part2 = load_plugin('provider', provider_definition)

                if self.config.get('resources') is None:
                    self.config['resources'] = {}

                for name, location in [
                    ("datastream", "schemas/connected-systems/datastream.schema"),
                    ("observation", "schemas/connected-systems/observation.schema")
                ]:
                    with open(location, 'r') as definition:
                        self.csa_schemas[name] = json.load(definition)
                    pass

    @process
    @jsonldify
    async def get_collections(self,
                              request: AsyncAPIRequest,
                              template: Tuple[dict, int, str],
                              original_format: str,
                              collection_id: str) -> Tuple[dict, int, str]:
        """
        Adds Connected-Systems collections to existing response
        """

        if self.csa_provider_part1 is None:
            # TODO: what to return here?
            raise NotImplementedError()

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

            parameters = parse_query_parameters(CollectionParams(), request.params)
            parameters.format = original_format
            data = await self.csa_provider_part1.query_collections(parameters)
        except ProviderItemNotFoundError:
            # element was not found in resources nor dynamic-resources, return 404
            if template[1] == HTTPStatus.NOT_FOUND:
                return template
            # else: there is already content

        headers = template[0]
        if data:
            if collection_id is not None:
                fcm = data[collection_id]
                if original_format == F_HTML:  # render
                    headers["Content-Type"] = "text/html"
                    content = render_j2_template(self.tpl_config,
                                                 'templates/connected-systems/collection/item.html',
                                                 fcm,
                                                 request.locale)
                    return headers, HTTPStatus.OK, content
                else:
                    headers["Content-Type"] = "application/json"
                    return headers, HTTPStatus.OK, to_json(fcm, self.pretty_print)
            else:
                fcm['collections'].extend(coll for _, coll in data.items())
                if original_format == F_HTML:  # render
                    fcm['collections_path'] = self.get_collections_url()
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

    @process
    @jsonldify
    async def landing_page(self,
                           request: AsyncAPIRequest) -> Tuple[dict, int, str]:
        """
        Provide API landing page

        :param request: A request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        fcm = {
            'links': [],
            'title': l10n.translate(
                self.config['metadata']['identification']['title'],
                request.locale),
            'description':
                l10n.translate(
                    self.config['metadata']['identification']['description'],
                    request.locale)
        }

        LOGGER.debug('Creating links')
        # TODO: put title text in config or translatable files?
        fcm['links'] = [{
            'rel': request.get_linkrel(F_JSON),
            'type': FORMAT_TYPES[F_JSON],
            'title': 'This document as JSON',
            'href': f"{self.base_url}?f={F_JSON}"
        }, {
            'rel': request.get_linkrel(F_JSONLD),
            'type': FORMAT_TYPES[F_JSONLD],
            'title': 'This document as RDF (JSON-LD)',
            'href': f"{self.base_url}?f={F_JSONLD}"
        }, {
            'rel': request.get_linkrel(F_HTML),
            'type': FORMAT_TYPES[F_HTML],
            'title': 'This document as HTML',
            'href': f"{self.base_url}?f={F_HTML}",
            'hreflang': self.default_locale
        }, {
            'rel': 'service-desc',
            'type': 'application/vnd.oai.openapi+json;version=3.0',
            'title': 'The OpenAPI definition as JSON',
            'href': f"{self.base_url}/openapi"
        }, {
            'rel': 'service-doc',
            'type': FORMAT_TYPES[F_HTML],
            'title': 'The OpenAPI definition as HTML',
            'href': f"{self.base_url}/openapi?f={F_HTML}",
            'hreflang': self.default_locale
        }, {
            'rel': 'conformance',
            'type': FORMAT_TYPES[F_JSON],
            'title': 'Conformance',
            'href': f"{self.base_url}/conformance"
        }, {
            'rel': 'data',
            'type': FORMAT_TYPES[F_JSON],
            'title': 'Collections',
            'href': self.get_collections_url()
        }]

        headers = request.get_response_headers(**self.api_headers)
        if request.format == F_HTML:  # render

            fcm['processes'] = False
            fcm['stac'] = False
            fcm['collection'] = False
            fcm['connected-systems'] = False

            if filter_dict_by_key_value(self.config['resources'],
                                        'type', 'process'):
                fcm['processes'] = True

            if filter_dict_by_key_value(self.config['resources'],
                                        'type', 'stac-collection'):
                fcm['stac'] = True

            if filter_dict_by_key_value(self.config['resources'],
                                        'type', 'collection'):
                fcm['collection'] = True

            if filter_dict_by_key_value(self.config['dynamic-resources'],
                                        'type', 'connected-systems'):
                fcm['connected-systems'] = True
                fcm['collection'] = True

            content = render_j2_template(self.tpl_config, 'templates/landing_page.html',
                                         fcm, request.locale)
            return headers, HTTPStatus.OK, content

        if request.format == F_JSONLD:
            return headers, HTTPStatus.OK, to_json(
                self.fcmld, self.pretty_print)

        return headers, HTTPStatus.OK, to_json(fcm, self.pretty_print)

    @process
    async def conformance(self,
                          request: AsyncAPIRequest) -> Tuple[dict, int, str]:
        """
        Provide conformance definition

        :param request: A request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        if not self.csa_provider_part1:
            return self.get_format_exception(request)

        conformance_list = self.csa_provider_part1.get_conformance()
        conformance = {
            'conformsTo': list(set(conformance_list))
        }

        headers = request.get_response_headers(**self.api_headers)
        if request.format == F_HTML:  # render
            content = render_j2_template(self.tpl_config, 'conformance.html',
                                         conformance, str(request.locale))
            return headers, HTTPStatus.OK, content

        return headers, HTTPStatus.OK, to_json(conformance, self.pretty_print)

    @process
    async def get_connected_systems_root(
            self, request: AsyncAPIRequest) -> Tuple[dict, int, str]:
        """
        Provide Connected Systems API root page

        :param request: APIRequest instance with query params

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers(**self.api_headers)

        id_ = 'pygeoapi-csa'
        version = '0.0.1'
        url = f'{self.base_url}'

        content = {
            'id': id_,
            'type': 'Catalog',
            'version': version,
            'title': l10n.translate(
                self.config['metadata']['identification']['title'],
                request.locale),
            'description': l10n.translate(
                self.config['metadata']['identification']['description'],
                request.locale),
            'links': [],
            'endpoints': []
        }

        collections = filter_dict_by_key_value(self.config['dynamic-resources'], 'type', 'connected-systems')

        for key, value in collections.items():
            content['links'].append({
                'rel': 'child',
                'href': f'{url}/{key}?f={F_JSON}',
                'type': FORMAT_TYPES[F_JSON]
            })
            content['links'].append({
                'rel': 'child',
                'href': f'{url}/{key}',
                'type': FORMAT_TYPES[F_HTML]
            })

        content['endpoints'].append({
            'title': f'Systems',
            'href': f'{url}/systems?f={FORMAT_TYPES[F_JSON].replace("+", "%2B")}',
            'type': F_JSON,
            'description': "Systems"
        })
        content['endpoints'].append({
            'title': f'Systems',
            'href': f'{url}/systems?f={FORMAT_TYPES[F_SENSORML_JSON].replace("+", "%2B")}',
            'type': F_SENSORML_JSON,
            'description': "Systems"
        })
        content['endpoints'].append({
            'title': f'Systems',
            'href': f'{url}/systems?f={FORMAT_TYPES[F_GEOJSON].replace("+", "%2B")}',
            'type': F_GEOJSON,
            'description': "Systems"
        })
        content['endpoints'].append({
            'title': f'Procedures',
            'href': f'{url}/procedures?f={FORMAT_TYPES[F_GEOJSON].replace("+", "%2B")}',
            'type': F_GEOJSON,
            'description': "Procedures provides information about the procedure implemented by a system to accomplish "
                           "its task(s)."
        })
        content['endpoints'].append({
            'title': f'Procedures',
            'href': f'{url}/procedures?f={FORMAT_TYPES[F_SENSORML_JSON].replace("+", "%2B")}',
            'type': F_SENSORML_JSON,
            'description': "Procedures provides information about the procedure implemented by a system to accomplish "
                           "its task(s)."
        })

        content['endpoints'].append({
            'title': f'Deployments',
            'href': f'{url}/deployments?f={FORMAT_TYPES[F_GEOJSON].replace("+", "%2B")}',
            'type': F_GEOJSON,
            'description': "Deployments describe how systems are being deployed at a particular place and time."
        })
        content['endpoints'].append({
            'title': f'Deployments',
            'href': f'{url}/deployments?f={FORMAT_TYPES[F_SENSORML_JSON].replace("+", "%2B")}',
            'type': F_SENSORML_JSON,
            'description': "Deployments describe how systems are being deployed at a particular place and time."
        })

        content['endpoints'].append({
            'title': f'SamplingFeatures',
            'href': f'{url}/samplingFeatures?f={FORMAT_TYPES[F_GEOJSON].replace("+", "%2B")}',
            'type': F_GEOJSON,
            'description': "Sampling Features link Systems with ultimate features of interest, describing exactly what part of a larger feature is being interacted with."
        })
        content['endpoints'].append({
            'title': f'Properties',
            'href': f'{url}/properties?f={FORMAT_TYPES[F_SENSORML_JSON].replace("+", "%2B")}',
            'type': F_SENSORML_JSON,
            'description': "List or search all Property resources available from this server endpoint."
        })
        content['endpoints'].append({
            'title': f'Datastreams',
            'href': f'{url}/datastreams?f={FORMAT_TYPES[F_JSON].replace("+", "%2B")}',
            'type': F_JSON,
            'description': "Datastreams allow access to observations produced by systems, in various formats."
        })
        content['endpoints'].append({
            'title': f'Observations',
            'href': f'{url}/observations?f={FORMAT_TYPES[F_OM_JSON].replace("+", "%2B")}',
            'type': F_OM_JSON,
            'description': "List or search all observations available from this server endpoint."
        })
        content['endpoints'].append({
            'title': f'Observations',
            'href': f'{url}/observations?f={FORMAT_TYPES[F_SWE_JSON].replace("+", "%2B")}',
            'type': F_SWE_JSON,
            'description': "List or search all observations available from this server endpoint."
        })

        if request.format == F_HTML:  # render
            content = render_j2_template(self.tpl_config,
                                         'templates/connected-systems/collection/overview.html',
                                         content, request.locale)
            return headers, HTTPStatus.OK, content

        return headers, HTTPStatus.OK, to_json(content, self.pretty_print)

    @process
    async def get_systems(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        return await self._handle_get(request,
                                      path,
                                      self.csa_provider_part1.query_systems,
                                      SystemsParams())

    @process
    async def get_procedures(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        return await self._handle_get(request,
                                      path,
                                      self.csa_provider_part1.query_procedures,
                                      ProceduresParams())

    @process
    async def get_deployments(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        return await self._handle_get(request,
                                      path,
                                      self.csa_provider_part1.query_deployments,
                                      DeploymentsParams())

    @process
    async def get_sampling_features(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        if request.format == FORMAT_TYPES[F_JSON] or request.format == FORMAT_TYPES[F_GEOJSON]:
            return await self._handle_get(request,
                                          path,
                                          self.csa_provider_part1.query_sampling_features,
                                          SamplingFeaturesParams())
        else:
            return self.get_format_exception(request)

    @process
    async def get_properties(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        return await self._handle_get(request,
                                      path,
                                      self.csa_provider_part1.query_properties,
                                      CSAParams())

    @process
    async def get_datastreams(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        return await self._handle_get(request,
                                      path,
                                      self.csa_provider_part2.query_datastreams,
                                      DatastreamsParams())

    @process
    async def get_datastreams_schema(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        if request.format == FORMAT_TYPES[F_JSON]:
            params = DatastreamsParams()
            params.schema = True
            return await self._handle_get(request,
                                          path,
                                          self.csa_provider_part2.query_datastreams,
                                          params)
        else:
            return self.get_format_exception(request)

    @process
    async def get_observations(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None) -> Tuple[dict, int, str]:
        return await self._handle_get(request,
                                      path,
                                      self.csa_provider_part2.query_observations,
                                      ObservationsParams())

    @process
    async def post_systems(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "system",
            self.csa_provider_part1,
            path
        )

    @process
    async def post_sampling_feature(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "samplingFeature",
            self.csa_provider_part1,
            path
        )

    @process
    async def post_deployments(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "deployment",
            self.csa_provider_part1,
            path
        )

    @process
    async def post_properties(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "property",
            self.csa_provider_part1,
            path
        )

    @process
    async def post_systems_to_deployment(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "system_link",
            self.csa_provider_part1,
            path
        )

    @process
    async def post_datastreams(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "datastream",
            self.csa_provider_part2,
            path
        )

    @process
    async def post_observations(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "observation",
            self.csa_provider_part2,
            path
        )

    @process
    async def post_procedures(
            self,
            request: AsyncAPIRequest,
            path: Union[Tuple[str, str], None] = None
    ) -> Tuple[dict, int, str]:
        return await self._handle_post(
            request,
            "procedure",
            self.csa_provider_part1,
            path
        )

    async def _handle_get(self,
                          request: AsyncAPIRequest,
                          path: Union[Tuple[str, str], None],
                          handler: Callable,
                          params: CSAParams
                          ) -> Tuple[dict, int, str]:
        """
        Provide Connected Systems API Collections

        :param request: APIRequest instance with query params
        :param path: Additional information extracted from path
        :param handler: Handler method of provider
        :param params: Parameter struct

        :returns: tuple of headers, status code, content
        """
        if self.csa_provider_part1 is None:
            # TODO: what to return here?
            raise NotImplementedError()

        if not request.is_valid(FORMAT_TYPES.values()):
            return self.get_format_exception(request)
        headers = request.get_response_headers(**self.api_headers)

        collection = True
        # Expand parameters with additional information based on path
        if path is not None:
            # Check that id is not malformed.
            if not re.match("^[\w-]+$", path[1]):
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

        # parse parameters
        try:
            parameters = parse_query_parameters(params, request.params)
            parameters.format = request.format
            data = await handler(parameters)

            return self._format_csa_response(request, headers, data, collection)
        except ProviderItemNotFoundError:
            return self.get_exception(
                HTTPStatus.NOT_FOUND,
                headers,
                request.format,
                'NotFound',
                "entity not found")

    async def _handle_post(
            self,
            request: AsyncAPIRequest,
            collection_name: str,
            provider: ConnectedSystemsProvider,
            path: Union[Tuple[str, str], None] = None,
    ) -> Tuple[dict, int, str]:

        # TODO: validate that POST is supported by provider
        # TODO: check format
        headers = request.get_response_headers(**self.api_headers)
        entities = json.loads(request.data)

        # unify posting single and multiple entities
        if type(entities) != list:
            entities = [entities]

        # Validate against json schema
        schema = self.csa_schemas[collection_name]
        try:
            for elem in entities:
                validate(instance=elem, schema=schema)
                if path is not None:
                    elem[path[0]] = path[1]
                else:
                    # remove additional fields that cannot be set using POST/PUT but only through reference in URL
                    # but passes validation
                    if "parent" in elem:
                        elem["parent"] = None

        except jsonschema.exceptions.ValidationError as ex:
            return self.get_exception(
                HTTPStatus.BAD_REQUEST,
                headers,
                request.format,
                'InvalidParameterValue',
                ex.message)

        try:
            # passthru to provider
            response = await provider.create(collection_name, entities)
        except Exception as ex:
            return self.get_exception(
                HTTPStatus.BAD_REQUEST,
                headers,
                request.format,
                'InvalidParameterValue',
                ex.args)

        return headers, HTTPStatus.OK, to_json(response, self.pretty_print)

    @process
    async def get_collection_items(
            self,
            request: AsyncAPIRequest,
            collection_id: str,
            item_id: str) -> Tuple[dict, int, str]:

        if self.csa_provider_part1 is None:
            # TODO: what to return here?
            raise NotImplementedError()

        headers = request.get_response_headers(**self.api_headers)
        try:
            request_params = request.params
            if item_id:
                request_params['id'] = item_id

            parameters = parse_query_parameters(CollectionParams(), request_params)
            data = await self.csa_provider_part1.query_collection_items(collection_id, parameters)
            return self._format_csa_response(request, headers, data, item_id is None)
        except ProviderItemNotFoundError:
            return headers, HTTPStatus.NOT_FOUND, ""

    def _format_csa_response(self, request, headers, data, is_collection: bool) -> Tuple[dict, int, str]:
        if FORMAT_TYPES.get(request.format):
            headers['Content-Type'] = FORMAT_TYPES.get(request.format)

        if data is None:
            return headers, HTTPStatus.NOT_FOUND, ""

        if len(data[0]) == 0:
            return headers, HTTPStatus.OK, "[]"

        if request.format == F_GEOJSON:
            response = {
                "type": "FeatureCollection",
                "features": [item for item in data[0]],
                "links": [link for link in data[1]],
            } if is_collection else data[0][0]
            return headers, HTTPStatus.OK, to_json(response, self.pretty_print)
        else:
            response = {
                "items": [item for item in data[0]],
                "links": [link for link in data[1]],
            } if is_collection else data[0][0]
            if request.format == F_HTML:
                # Some nicer formatting
                pretty = f"""
                <html><body><pre><code>
                {to_json(response, True)}
                </code></pre></body></html>
                """
                return headers, HTTPStatus.OK, pretty
            else:
                return headers, HTTPStatus.OK, to_json(response, self.pretty_print)
