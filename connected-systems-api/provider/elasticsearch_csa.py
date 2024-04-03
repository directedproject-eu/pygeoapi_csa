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
import asyncio
import datetime
import json
import logging
import uuid
from typing import Dict, List, Union, Coroutine

from elastic_transport import NodeConfig
from elasticsearch import AsyncElasticsearch
from elasticsearch_dsl import Search

from pygeoapi.provider.base import ProviderConnectionError, ProviderQueryError, ProviderInvalidDataError, \
    ProviderGenericError

from .connectedsystems import ConnectedSystemsBaseProvider, CSACrudResponse, SystemsParams, \
    CSAGetResponse, DeploymentsParams, ProceduresParams, SamplingFeaturesParams, CSAParams, CommonParams, \
    CollectionParams

LOGGER = logging.getLogger(__name__)


def parse_common_params(query: Search, parameters: CommonParams) -> Search:
    # Parse dateTime filter
    if parameters.datetime_start() and parameters.datetime_end():
        query = query.filter("range", validTime_parsed={"gte": parameters.datetime_start().isoformat(),
                                                       "lte": parameters.datetime_end().isoformat()})
    if parameters.datetime_start():
        query = query.filter("range", validTime_parsed={"gte": parameters.datetime_start().isoformat()})
    if parameters.datetime_end():
        query = query.filter("range", validTime_parsed={"lte": parameters.datetime_end().isoformat()})

    if parameters.foi:
        LOGGER.critical("not implemented!")
        raise ProviderQueryError("not implemented")
    if parameters.observedProperty:
        LOGGER.critical("not implemented!")
        raise ProviderQueryError("not implemented")
    return query


def parse_csa_params(query: Search, parameters: CSAParams) -> Search:
    # Parse id filter
    if parameters.id is not None:
        query = query.filter("terms", _id=parameters.id)
    if parameters.q is not None:
        query = query.query("multi_match", query=parameters.q, fields=["name", "description"])

    if parameters.offset != 0:
        LOGGER.critical("not implemented!")
        raise ProviderQueryError("not implemented")
    return query


def parse_spatial_params(query: Search,
                         parameters: Union[
                             DeploymentsParams, SystemsParams, SamplingFeaturesParams, CollectionParams]) -> Search:
    # Parse bbox filter
    if parameters.bbox is not None:
        br = f"POINT ({parameters.bbox['y1']} {parameters.bbox['x2']})"
        tl = f"POINT ({parameters.bbox['x1']} {parameters.bbox['y2']})"
        query = query.filter("geo_bounding_box", position={"top_left": tl, "bottom_right": br})
    if parameters.geom is not None:
        query = query.filter("geo_shape", position={"relation": "intersects", "shape": parameters.geom})
    return query


class ConnectedSystemsESProvider(ConnectedSystemsBaseProvider):
    collections_index_name = "collections"
    systems_index_name = "systems"
    deployments_index_name = "deployments"
    procedures_index_name = "procedures"
    samplingfeatures_index_name = "sampling_features"
    properties_index_name = "properties"

    datastreams_index_name = "datastreams"

    # TODO: check if there are further problematic fields
    common_mappings = {
        "properties": {
            "id": {
                "type": "keyword"
            }
        }
    }
    system_mappings = common_mappings | {
        "properties": {
            "characteristics": {
                "properties": {
                    "characteristics": {
                        "properties": {
                            "value": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            },
            "position": {
                "type": "geo_shape"
            },
            "validTime_parsed": {
                "type": "date_range"
            },
            "parent": {
                "type": "keyword"
            },
            "procedure": {
                "type": "keyword"
            },
            "poi": {
                "type": "keyword"
            },
            "observedProperty": {
                "type": "keyword"
            },
            "controlledProperty": {
                "type": "keyword"
            },
            "uniqueId": {
                "type": "keyword"
            }
        }
    }

    deployments_mappings = common_mappings
    procedures_mappings = common_mappings
    samplingfeatures_mappings = common_mappings
    properties_mappings = common_mappings

    def __init__(self, provider_def: Dict):
        super().__init__(provider_def)
        self.es_host = provider_def['host']
        self.es_port = provider_def['port']
        self.es_path = provider_def['path']

        LOGGER.debug('Setting Elasticsearch properties')
        LOGGER.debug('Connecting to Elasticsearch at: https://{self.es_host}:{self.es_port}/{self.es_host}')

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.__setup_es(provider_def))

    def get_conformance(self) -> List[str]:
        # TODO: check which of these we actually support
        return [
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/html",
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-common-2/0.0/conf/collections",
            "http://www.opengis.net/spec/ogcapi-common-2/0.0/conf/html",
            "http://www.opengis.net/spec/ogcapi-common-2/0.0/conf/json",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/html",
            "http://www.opengis.net/spec/ogcapi-features-4/1.0/conf/create-replace-delete",
            "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/system-features",
            "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/procedure-features",
            "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/deployment-features",
            "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/sampling-features",
            "http://www.opengis.net/spec/ogcapi-connectedsystems-2/1.0/conf/encoding/geojson",
            "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/encoding/sensorml"
        ]

    async def __setup_es(self, provider_def: Dict):
        self.es: AsyncElasticsearch = AsyncElasticsearch(
            [
                NodeConfig(
                    scheme="https",
                    host=self.es_host,
                    port=self.es_port,
                    verify_certs=False,
                    ca_certs=None,
                    ssl_show_warn=False,
                )
            ],
            http_auth=(provider_def['user'], provider_def['password']),
            verify_certs=False)
        if not await self.es.ping():
            msg = f'Cannot connect to Elasticsearch: {self.es_host}'
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        LOGGER.debug('Determining ES version')
        v = await(self.es.info())
        v = v['version']['number'][:3]
        if float(v) < 8:
            msg = 'only ES 8+ supported'
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        # TODO: remove
        try:
            for index in [(self.systems_index_name, self.system_mappings),
                          (self.collections_index_name, None),
                          (self.procedures_index_name, self.procedures_mappings),
                          (self.deployments_index_name, self.deployments_mappings),
                          (self.properties_index_name, self.properties_mappings),
                          (self.samplingfeatures_index_name, self.samplingfeatures_mappings),
                          ]:
                index_name, index_mapping = index
                if not await (self.es.indices.exists(index=index_name)):
                    await self.es.indices.create(
                        index=index_name,
                        mappings=index_mapping
                    )
        except Exception as e:
            LOGGER.exception(e)

        await self.__create_mandatory_collections()
        LOGGER.critical("finished initializing csa-es'")

    async def __create_mandatory_collections(self):
        # Create mandatory collections if not exists

        mandatory = [
            {
                "id": "all_systems",
                "type": "collection",
                "title": "All Connected Systems",
                "description": "All systems registered on this server (e.g. platforms, sensors, actuators, processes)",
                "itemType": "feature",
                "featureType": "system",
                "links": [
                    {
                        "rel": "self",
                        "title": "This document",
                        "href": "collections/all_systems",
                        "type": "application/json"
                    },
                    {
                        "rel": "items",
                        "title": "Access the system instances in this collection",
                        "href": "systems",
                        "type": "application/json"
                    }
                ]
            },
            {
                "id": "all_datastreams",
                "type": "collection",
                "title": "All Systems Datastreams",
                "description": "All datastreams produced by systems registered on this server",
                "itemType": "feature",
                "featureType": "datastreams",
                "links": [
                    {
                        "rel": "self",
                        "title": "This document",
                        "href": "collections/all_datastreams",
                        "type": "application/json"
                    },
                    {
                        "rel": "items",
                        "title": "Access the datastreams in this collection",
                        "href": "datastreams",
                        "type": "application/json"
                    }
                ]
            },
            {
                "id": "all_fois",
                "type": "collection",
                "title": "All Features of Interest",
                "description": "All features of interest observed or affected by systems registered on this server",
                "itemType": "feature",
                "featureType": "featureOfInterest",
                "links": [
                    {
                        "rel": "self",
                        "title": "This document",
                        "href": "collections/all_fois",
                        "type": "application/json"
                    },
                    {
                        "rel": "items",
                        "title": "Access the features of interests in this collection",
                        "href": "featuresOfInterest",
                        "type": "application/json"
                    }
                ]
            },
            {
                "id": "all_procedures",
                "type": "collection",
                "title": "All Procedures and System Datasheets",
                "description": "All procedures (e.g. system datasheets) implemented by systems registered on this server",
                "itemType": "feature",
                "featureType": "procedure",
                "links": [
                    {
                        "rel": "self",
                        "title": "This document",
                        "href": "collections/all_procedures",
                        "type": "application/json"
                    },
                    {
                        "rel": "items",
                        "title": "Access the procedures in this collection",
                        "href": "procedures",
                        "type": "application/json"
                    }
                ]
            }
        ]

        for coll in mandatory:
            query = Search(using=self.es, index=self.collections_index_name)
            query = query.filter("term", id=coll["id"])
            result = (await self.es.search(index=self.collections_index_name, body=query.to_dict()))["hits"]
            if len(result["hits"]) == 0:
                await self.es.index(index=self.collections_index_name,
                                    id=coll["id"],
                                    document=coll,
                                    refresh=True)
            LOGGER.critical(f"creating mandatory collection {coll['id']}")

    def query_collections(self, parameters: CollectionParams) -> CSAGetResponse:
        return self.loop.run_until_complete(self._query_collections(parameters))

    def query_collection_items(self, collection_id: str, parameters: CSAParams) -> CSAGetResponse:
        return self.loop.run_until_complete(self._query_collection_items(collection_id, parameters))

    def query_systems(self, parameters: SystemsParams) -> CSAGetResponse:
        return self.loop.run_until_complete(self._query_systems(parameters))

    def query_deployments(self, parameters: DeploymentsParams) -> CSAGetResponse:
        return self.loop.run_until_complete(self._query_deployments(parameters))

    def query_procedures(self, parameters: ProceduresParams) -> CSAGetResponse:
        return self.loop.run_until_complete(self._query_procedures(parameters))

    def query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAGetResponse:
        return self.loop.run_until_complete(self._query_sampling_features(parameters))

    def query_properties(self, parameters: CSAParams) -> CSAGetResponse:
        return self.loop.run_until_complete(self._query_properties(parameters))

    def create(self, type: str, items: List[Dict]) -> CSACrudResponse:
        return self.loop.run_until_complete(self._create(type, items))

    async def _query_collections(self, parameters: CollectionParams) -> Dict[str, Dict]:
        query = Search(using=self.es, index=self.collections_index_name)

        query = parse_csa_params(query, parameters)
        query = parse_spatial_params(query, parameters)

        found = (await self.es.search(index=self.collections_index_name,
                                      body=query.to_dict()))["hits"]
        collections = {}
        if found["total"]["value"] > 0:
            for h in found["hits"]:
                collections[h["_source"]["id"]] = h["_source"]
        return collections

    async def _query_collection_items(self, collection_id: str, parameters: CSAParams) -> CSAGetResponse:
        # TODO: implement this for non-mandatory collections
        if collection_id == "all_systems":
            index = self.systems_index_name
        elif collection_id == "all_procedures":
            index = self.procedures_index_name
        elif collection_id == "all_fois":
            index = self.samplingfeatures_index_name
        elif collection_id == "all_datastreams":
            index = self.datastreams_index_name
        else:
            # TODO: maybe throw an error here?
            return [], []

        query = Search(using=self.es, index=index)

        if parameters.id:
            query = query.filter("terms", _id=parameters.id)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await self._search(index, query.to_dict(), parameters)

    async def _query_systems(self, parameters: SystemsParams) -> CSAGetResponse:
        query = Search(using=self.es, index=self.systems_index_name)

        query = parse_common_params(query, parameters)
        query = parse_csa_params(query, parameters)
        query = parse_spatial_params(query, parameters)

        if parameters.geom is not None:
            query = query.filter("geo_shape", position={"relation": "intersects", "shape": parameters.geom})

        # By default, only top level systems are included (i.e. subsystems are ommitted)
        # unless the parent query parameter is set.
        if parameters.parent is not None:
            query = query.filter("terms", **{"parent": parameters.parent})
        else:
            query = query.exclude("exists", field="parent")

        for key in ["procedure", "foi", "observedProperty", "controlledProperty"]:
            prop = parameters.__getattribute__(key)
            if prop is not None:
                query = query.filter("terms", **{key: prop})

        # DEBUG only
        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await self._search(self.systems_index_name, query.to_dict(), parameters, ["validTime_parsed"])

    async def _query_deployments(self, parameters: DeploymentsParams) -> CSAGetResponse:
        query = Search(using=self.es, index=self.deployments_index_name)

        query = parse_common_params(query, parameters)
        query = parse_csa_params(query, parameters)
        query = parse_spatial_params(query, parameters)

        if parameters.system is not None:
            query = query.filter("terms", system=parameters.system)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await self._search(self.deployments_index_name, query.to_dict(), parameters)

    async def _query_procedures(self, parameters: ProceduresParams) -> CSAGetResponse:
        query = Search(using=self.es, index=self.procedures_index_name)

        query = parse_common_params(query, parameters)
        query = parse_csa_params(query, parameters)

        if parameters.controlledProperty is not None:
            # TODO: check if this is the correct property
            query = query.filter("terms", controlledProperty=parameters.controlledProperty)

        json.dumps(query.to_dict(), indent=True, default=str)
        return await self._search(self.procedures_index_name, query.to_dict(), parameters)

    async def _query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAGetResponse:
        query = Search(using=self.es, index=self.samplingfeatures_index_name)

        query = parse_common_params(query, parameters)
        query = parse_csa_params(query, parameters)

        if parameters.controlledProperty is not None:
            # TODO: check if this is the correct property
            query = query.filter("terms", controlledProperty=parameters.controlledProperty)

        if parameters.system is not None:
            query = query.filter("terms", system=parameters.system)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await self._search(self.samplingfeatures_index_name, query.to_dict(), parameters)

    async def _query_properties(self, parameters: CSAParams) -> CSAGetResponse:
        query = Search(using=self.es, index=self.properties_index_name)

        query = parse_csa_params(query, parameters)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await self._search(self.properties_index_name, query.to_dict(), parameters)

    async def _search(self, index: str, body: Dict, parameters: CSAParams, excludes=None) -> CSAGetResponse:
        # Select appropriate strategy here: For collections >10k elements search_after must be used
        if excludes is None:
            excludes = []
        found = (await self.es.search(body=body,
                                      index=index,
                                      size=parameters.limit,
                                      source_excludes=excludes))["hits"]

        if found["total"]["value"] > 0:
            return [h["_source"] for h in found["hits"]], []
        else:

            # check if this query returns 404 or 200 with empty body in case of no return
            if parameters.id:
                return None
            else:
                return [], []

    async def _create(self, type: str, items: List[Dict]) -> CSACrudResponse:
        routines = [None] * len(items)

        for index, item in enumerate(items):
            if type == "system":
                # parse date_range fields to es-compatible format
                self._format_date_range("validTime", item)
                index_name = self.systems_index_name
            elif type == "deployment":
                index_name = self.deployments_index_name
            elif type == "procedure":
                index_name = self.procedures_index_name
            elif type == "samplingFeature":
                index_name = self.samplingfeatures_index_name
            elif type == "property":
                index_name = self.properties_index_name
            else:
                raise ProviderGenericError(f"unrecognized type: {type}")

            # add to ES if not already present

            identifier = None
            if "id" not in item:
                identifier = str(uuid.uuid4())
                item["id"] = identifier
            else:
                identifier = item["id"]

            routines[index] = self._create_if_not_exists(index_name, item, identifier)

        # wait for completion
        await asyncio.gather(*routines)

        return [item["id"] for item in items]

    async def _create_if_not_exists(self, index: str, item: Dict, identifier: str) -> Coroutine:
        exists = await self.es.exists(index=index, id=identifier)
        if exists.body:
            msg = 'record already exists'
            LOGGER.error(msg)
            raise ProviderInvalidDataError(msg)
        else:
            return await self.es.index(index=index, id=identifier, document=item)

    def _format_date_range(self, key: str, item: Dict) -> None:
        if item.get(key):
            time = item.get(key)
            now = datetime.datetime.utcnow()
            if time[0] == "now":
                start = now
            else:
                start = time[0]
            if time[1] == "now":
                end = now
            else:
                end = time[1]

            item[key + "_parsed"] = {
                "gte": start,
                "lte": end
            }