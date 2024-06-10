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
from typing import Dict, List, Union, Coroutine, Tuple

from elasticsearch import AsyncElasticsearch
from elasticsearch_dsl import Search

from pygeoapi.provider.base import ProviderConnectionError, ProviderQueryError, ProviderInvalidDataError, \
    ProviderGenericError

from ..definitions import ConnectedSystemsPart1Provider, CSACrudResponse, SystemsParams, \
    CSAGetResponse, DeploymentsParams, ProceduresParams, SamplingFeaturesParams, CSAParams, CollectionParams

from ..connector_elastic import connect_elasticsearch, ElasticSearchConfig, parse_csa_params, parse_spatial_params, \
    parse_datetime_params, create_many, search

LOGGER = logging.getLogger(__name__)


class ConnectedSystemsESProvider(ConnectedSystemsPart1Provider):
    _es: AsyncElasticsearch = None

    collections_index_name = "collections"
    systems_index_name = "systems"
    deployments_index_name = "deployments"
    procedures_index_name = "procedures"
    samplingfeatures_index_name = "sampling_features"
    properties_index_name = "properties"

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
        self._es_config = ElasticSearchConfig(
            hostname=provider_def['host'],
            port=int(provider_def['port']),
            dbname=provider_def['dbname'],
            user=provider_def['user'],
            password=provider_def['password']
        )

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

    async def open(self):

        self._es: AsyncElasticsearch = await connect_elasticsearch(
            self._es_config,
            [(self.systems_index_name, self.system_mappings),
             (self.collections_index_name, None),
             (self.procedures_index_name,
              self.procedures_mappings),
             (self.deployments_index_name,
              self.deployments_mappings),
             (self.properties_index_name,
              self.properties_mappings),
             (self.samplingfeatures_index_name,
              self.samplingfeatures_mappings),
             ])
        await self.__create_mandatory_collections()

    async def close(self):
        await self._es.close()

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
            query = Search(using=self._es, index=self.collections_index_name)
            query = query.filter("term", id=coll["id"])
            result = (await self._es.search(index=self.collections_index_name, body=query.to_dict()))["hits"]
            if len(result["hits"]) == 0:
                await self._es.index(index=self.collections_index_name,
                                     id=coll["id"],
                                     document=coll,
                                     refresh=True)
            LOGGER.critical(f"creating mandatory collection {coll['id']}")

    async def query_collections(self, parameters: CollectionParams) -> Dict[str, Dict]:
        query = Search(using=self._es, index=self.collections_index_name)

        query = parse_csa_params(query, parameters)
        query = parse_spatial_params(query, parameters)

        found = (await self._es.search(index=self.collections_index_name,
                                       body=query.to_dict()))["hits"]
        collections = {}
        if found["total"]["value"] > 0:
            for h in found["hits"]:
                collections[h["_source"]["id"]] = h["_source"]
        return collections

    async def query_collection_items(self, collection_id: str, parameters: CSAParams) -> CSAGetResponse:
        # TODO: implement this for non-mandatory collections
        if collection_id == "all_systems":
            index = self.systems_index_name
        elif collection_id == "all_procedures":
            index = self.procedures_index_name
        elif collection_id == "all_fois":
            index = self.samplingfeatures_index_name
        else:
            # TODO: maybe throw an error here?
            return [], []

        query = Search(using=self._es, index=index)

        if parameters.id:
            query = query.filter("terms", _id=parameters.id)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await search(self._es, index, query.to_dict(), parameters)

    async def query_systems(self, parameters: SystemsParams) -> CSAGetResponse:
        query = Search(using=self._es, index=self.systems_index_name)

        query = parse_datetime_params(query, parameters)
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
        return await search(self._es, self.systems_index_name, query.to_dict(), parameters, ["validTime_parsed"])

    async def query_deployments(self, parameters: DeploymentsParams) -> CSAGetResponse:
        query = Search(using=self._es, index=self.deployments_index_name)

        query = parse_datetime_params(query, parameters)
        query = parse_csa_params(query, parameters)
        query = parse_spatial_params(query, parameters)

        if parameters.system is not None:
            query = query.filter("terms", system=parameters.system)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await search(self._es, self.deployments_index_name, query.to_dict(), parameters)

    async def query_procedures(self, parameters: ProceduresParams) -> CSAGetResponse:
        query = Search(using=self._es, index=self.procedures_index_name)

        query = parse_datetime_params(query, parameters)
        query = parse_csa_params(query, parameters)

        if parameters.controlledProperty is not None:
            # TODO: check if this is the correct property
            query = query.filter("terms", controlledProperty=parameters.controlledProperty)

        json.dumps(query.to_dict(), indent=True, default=str)
        return await search(self._es, self.procedures_index_name, query.to_dict(), parameters)

    async def query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAGetResponse:
        query = Search(using=self._es, index=self.samplingfeatures_index_name)

        query = parse_datetime_params(query, parameters)
        query = parse_csa_params(query, parameters)

        if parameters.controlledProperty is not None:
            # TODO: check if this is the correct property
            query = query.filter("terms", controlledProperty=parameters.controlledProperty)

        if parameters.system is not None:
            query = query.filter("terms", system=parameters.system)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await search(self._es, self.samplingfeatures_index_name, query.to_dict(), parameters)

    async def query_properties(self, parameters: CSAParams) -> CSAGetResponse:
        query = Search(using=self._es, index=self.properties_index_name)

        query = parse_csa_params(query, parameters)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        return await search(self._es, self.properties_index_name, query.to_dict(), parameters)

    async def create(self, type: str, items: List[Dict]) -> CSACrudResponse:
        routines: List[Tuple[str, Dict]] = [None] * len(items)

        for i, item in enumerate(items):
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

            if "id" not in item:
                # We may have to generate id as it is not always required
                identifier = str(uuid.uuid4())
                item["id"] = identifier
            else:
                identifier = item["id"]

            routines[i] = (identifier, item)

        return await create_many(self._es, index_name, routines)

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
