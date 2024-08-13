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
import datetime
import logging
import uuid
from typing import Dict, List

import elasticsearch
from elasticsearch_dsl import AsyncSearch, AsyncDocument, Keyword, GeoShape, DateRange, InnerDoc, \
    async_connections
from pygeoapi.provider.base import ProviderGenericError, ProviderInvalidQueryError, ProviderNotFoundError

from ..connector_elastic import ElasticsearchConnector, ElasticSearchConfig, parse_csa_params, parse_spatial_params, \
    parse_datetime_params
from ..definitions import *

LOGGER = logging.getLogger(__name__)
# LOGGER.setLevel(level='DEBUG')


class ConnectedSystemsESProvider(ConnectedSystemsPart1Provider, ElasticsearchConnector):

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
        ]

    async def open(self):
        await self.connect_elasticsearch(self._es_config)

    async def setup(self):
        await System.init()
        await Deployment.init()
        await Procedure.init()
        await SamplingFeature.init()
        await Property.init()
        await self.__create_mandatory_collections()

    async def close(self):
        es = async_connections.get_connection()
        await es.close()

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
                        "title": "This document (JSON)",
                        "href": "/collections/all_systems",
                        "type": "application/json"
                    },
                    {
                        "rel": "self",
                        "title": "This document (HTML)",
                        "href": "/collections/all_systems?f=html",
                        "type": "text/html"
                    },
                    {
                        "rel": "items",
                        "title": "Access the system instances in this collection (HTML)",
                        "href": "/systems",
                        "type": "text/html"
                    },
                    {
                        "rel": "items",
                        "title": "Access the system instances in this collection (JSON)",
                        "href": "/systems?f=application/json",
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
                        "title": "This document (JSON)",
                        "href": "/collections/all_datastreams",
                        "type": "application/json"
                    },
                    {
                        "rel": "self",
                        "title": "This document (HTML)",
                        "href": "/collections/all_datastreams?f=html",
                        "type": "application/json"
                    },
                    {
                        "rel": "items",
                        "title": "Access the datastreams in this collection (HTML)",
                        "href": "/datastreams",
                        "type": "text/html"
                    },
                    {
                        "rel": "items",
                        "title": "Access the datastreams in this collection (JSON)",
                        "href": "/datastreams?f=application/json",
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
                        "title": "This document (JSON)",
                        "href": "/collections/all_fois",
                        "type": "application/json"
                    },
                    {
                        "rel": "self",
                        "title": "This document (HTML)",
                        "href": "/collections/all_fois?f=html",
                        "type": "text/html"
                    },
                    {
                        "rel": "items",
                        "title": "Access the features of interests in this collection (HTML)",
                        "href": "/featuresOfInterest",
                        "type": "text/html"
                    },
                    {
                        "rel": "items",
                        "title": "Access the features of interests in this collection (JSON)",
                        "href": "/featuresOfInterest?f=json",
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
                        "title": "This document (JSON)",
                        "href": "/collections/all_procedures",
                        "type": "application/json"
                    },
                    {
                        "rel": "self",
                        "title": "This document (HTML)",
                        "href": "/collections/all_procedures?f=html",
                        "type": "application/json"
                    },
                    {
                        "rel": "items",
                        "title": "Access the procedures in this collection (HTML)",
                        "href": "procedures",
                        "type": "text/html"
                    },
                    {
                        "rel": "items",
                        "title": "Access the procedures in this collection (JSON)",
                        "href": "procedures?f=application/json",
                        "type": "application/json"
                    }
                ]
            }
        ]

        for coll in mandatory:
            query = AsyncSearch(index=self.collections_index_name)
            query = query.filter("term", id=coll["id"])
            result = (await self._es.search(index=self.collections_index_name, body=query.to_dict()))["hits"]
            if len(result["hits"]) == 0:
                await self._es.index(index=self.collections_index_name,
                                     id=coll["id"],
                                     document=coll,
                                     refresh=True)
            LOGGER.critical(f"creating mandatory collection {coll['id']}")

    # async def query_collections(self, parameters: CollectionParams) -> Dict[str, Dict]:
    #     query = AsyncSearch(index=self.collections_index_name)
    #
    #     query = parse_csa_params(query, parameters)
    #     query = parse_spatial_params(query, parameters)
    #
    #
    #     found = (await self._es.search(query))["hits"]
    #     collections = {}
    #     if found["total"]["value"] > 0:
    #         for h in found["hits"]:
    #             collections[h["_source"]["id"]] = h["_source"]
    #     return collections

    # async def query_collection_items(self, collection_id: str, parameters: CSAParams) -> CSAGetResponse:
    #     # TODO: implement this for non-mandatory collections
    #     if collection_id == "all_systems":
    #         index = self.systems_index_name
    #     elif collection_id == "all_procedures":
    #         index = self.procedures_index_name
    #     elif collection_id == "all_fois":
    #         index = self.samplingfeatures_index_name
    #     else:
    #         # TODO: maybe throw an error here?
    #         return [], []
    #
    #     query = AsyncSearch(index=index)
    #
    #     if parameters.id:
    #         query = query.filter("terms", _id=parameters.id)
    #
    #     LOGGER.debug(json.dumps(query, indent=True, default=str))
    #     return await self.search(index, query, parameters)

    async def query_systems(self, parameters: SystemsParams) -> CSAGetResponse:
        query = System.search()

        query = parse_datetime_params(query, parameters)
        query = parse_csa_params(query, parameters)
        query = parse_spatial_params(query, parameters)

        if parameters.geom is not None:
            query = query.filter("geo_shape", position={"relation": "intersects", "shape": parameters.geom})

        # By default, only top level systems are included (i.e. subsystems are ommitted)
        # unless query parameter 'parent' or 'id' is set
        if parameters.parent is not None:
            query = query.filter("terms", **{"parent": parameters.parent})
        else:
            pass
            # When requested as a collection
            if not parameters.id:
                query = query.exclude("exists", field="parent")

        for key in ["procedure", "foi", "observedProperty", "controlledProperty"]:
            prop = parameters.__getattribute__(key)
            if prop is not None:
                query = query.filter("terms", **{key: prop})

        return await self.search(query, parameters, ["validTime_parsed"])

    async def query_deployments(self, parameters: DeploymentsParams) -> CSAGetResponse:
        query = Deployment.search()

        query = parse_datetime_params(query, parameters)
        query = parse_csa_params(query, parameters)
        query = parse_spatial_params(query, parameters)

        if parameters.system is not None:
            query = query.filter("terms", system=parameters.system)

        return await self.search(query, parameters)

    async def query_procedures(self, parameters: ProceduresParams) -> CSAGetResponse:
        query = Procedure.search()

        query = parse_datetime_params(query, parameters)
        query = parse_csa_params(query, parameters)

        if parameters.controlledProperty is not None:
            # TODO: check if this is the correct property
            query = query.filter("terms", controlledProperty=parameters.controlledProperty)

        return await self.search(query, parameters)

    async def query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAGetResponse:
        query = SamplingFeature.search()

        query = parse_datetime_params(query, parameters)
        query = parse_csa_params(query, parameters)

        if parameters.controlledProperty is not None:
            # TODO: check if this is the correct property
            query = query.filter("terms", controlledProperty=parameters.controlledProperty)

        if parameters.system is not None:
            query = query.filter("terms", system=parameters.system)

        return await self.search(query, parameters)

    async def query_properties(self, parameters: CSAParams) -> CSAGetResponse:
        query = Property.search()
        query = parse_csa_params(query, parameters)

        return await self.search(query, parameters)

    async def create(self, type: EntityType, item: Dict) -> CSACrudResponse:

        # Special Handling for some fields
        match type:
            case EntityType.SYSTEMS:
                # parse date_range fields to es-compatible format
                self._format_date_range("validTime", item)
                parent_id = item.get("parent", None)
                if parent_id and not self._exists(System.search().filter("term", id=parent_id)):
                    # check that parent exists,
                    raise ProviderInvalidQueryError(user_msg=f"cannot find parent system with id: {parent_id}")
                entity = System(**item)
            case EntityType.DEPLOYMENTS:
                entity = Deployment(**item)
            case EntityType.PROCEDURES:
                entity = Procedure(**item)
            case EntityType.SAMPLING_FEATURES:
                entity = SamplingFeature(**item)
            case EntityType.PROPERTIES:
                entity = Property(**item)
            case _:
                raise ProviderInvalidQueryError(user_msg=f"unrecognized type {type}")

        if "id" not in item:
            # We may have to generate id as it is not always required
            identifier = str(uuid.uuid4())
            entity.id = identifier
        else:
            identifier = item["id"]

        try:
            if await entity.save():
                return identifier
            else:
                raise Exception("cannot save identifier!")
        except Exception as e:
            raise ProviderInvalidQueryError(user_msg=str(e))

    async def replace(self, type: EntityType, identifier: str, item: Dict):
        LOGGER.debug(f"replacing {type} {identifier}")
        old = await self._get_entity(type, identifier)
        new = System(**item)
        new.meta.id = old.meta.id
        await new.save()

    async def update(self, type: EntityType, identifier: str, item: Dict):
        LOGGER.debug(f"updating {type} {identifier}")
        await (await self._get_entity(type, identifier)).update(**item)

    async def delete(self, type: EntityType, identifier: str, cascade: bool = False):
        LOGGER.debug(f"deleting {type} {identifier}")
        try:
            match type:
                case EntityType.SYSTEMS:
                    if not cascade:
                        # /req/create-replace-delete/system
                        # reject if there are nested resources: subsystems, sampling features, datastreams, control streams
                        error_msg = f"cannot delete system with nested resources and cascade=false. "
                        f"ref: /req/create-replace-delete/system"

                        # TODO: Should we run all these checks in parallel or is it more efficient to sync + exit early?
                        # check subsystems
                        if await self._exists(System.search().filter("term", parent=identifier)):
                            raise ProviderInvalidQueryError(user_msg=error_msg)

                        # check deployments
                        if await self._exists(Deployment.search().filter("term", system=identifier)):
                            raise ProviderInvalidQueryError(user_msg=error_msg)

                        # check sampling features
                        if await self._exists(
                                SamplingFeature.search().filter("term", system=identifier)):
                            raise ProviderInvalidQueryError(user_msg=error_msg)

                        entity = await System.get(identifier)
                        # if self._provider_part2:

                    else:
                        # blocked-by: https://github.com/opengeospatial/ogcapi-connected-systems/issues/61
                        # /req/create-replace-delete/system-delete-cascade
                        # async with asyncio.TaskGroup() as tg:
                        #     # recursively delete subsystems with all their associated entities
                        #     subsystems = (AsyncSearch(using=self._es)
                        #                   .index(self.systems_index_name)
                        #                   .filter("term", parent=identifier)
                        #                   .source(False)
                        #                   .scan())
                        #     #async for subsystem in subsystems:
                        #     #    tg.create_task(self.delete("system", subsystem.meta.id, True))
                        #     print((AsyncSearch(using=self._es)
                        #      .index(self.deployments_index_name)
                        #      .filter("term", system=identifier)
                        #      .source(False)).to_dict())
                        #     deployments = (AsyncSearch(using=self._es)
                        #                    .index(self.deployments_index_name)
                        #                    .filter("term", system=identifier)
                        #                    .source(False)
                        #                    .scan())
                        #     async for d in deployments:
                        #         print("deleting deployment?")
                        #         tg.create_task(self.delete("deployment", d.meta.id, True))
                        #
                        #     samplingfeatures = (AsyncSearch(using=self._es)
                        #                         .index(self.samplingfeatures_index_name)
                        #                         .filter("term", system=identifier)
                        #                         .source(False)
                        #                         .scan())
                        #     async for s in samplingfeatures:
                        #         tg.create_task(self.delete("samplingFeature", s.meta.id, True))
                        #
                        # # await self._delete(self.systems_index_name, identifier)
                        return ProviderGenericError("cascade=true is not implemented yet!")
                    entity = await System.get(id=identifier)
                case EntityType.DEPLOYMENTS:
                    entity = await Deployment.get(id=identifier)
                case EntityType.PROCEDURES:
                    entity = await Procedure.get(id=identifier)
                case EntityType.SAMPLING_FEATURES:
                    entity = await SamplingFeature.get(id=identifier)
                case EntityType.PROPERTIES:
                    entity = await Property.get(id=identifier)
                case _:
                    raise ProviderInvalidQueryError(user_msg=f"unrecognized type {type}")
            return await entity.delete()
        except elasticsearch.NotFoundError as e:
            raise ProviderNotFoundError(user_msg=f"cannot find {type} with id: {identifier}! {e}")
        except Exception as e:
            raise ProviderGenericError(user_msg=f"error while deleting: {e}")

    async def _get_entity(self, type: EntityType, identifier: str):
        try:
            match type:
                case EntityType.SYSTEMS:
                    entity = System.get(id=identifier)
                case EntityType.DEPLOYMENTS:
                    entity = Deployment.get(id=identifier)
                case EntityType.PROCEDURES:
                    entity = Procedure.get(id=identifier)
                case EntityType.SAMPLING_FEATURES:
                    entity = SamplingFeature.get(id=identifier)
                case EntityType.PROPERTIES:
                    entity = Property.get(id=identifier)
                case _:
                    raise ProviderInvalidQueryError(user_msg=f"unrecognized type {type}")
            return await entity
        except Exception as e:
            raise ProviderNotFoundError(user_msg=f"cannot find {type} with id: {identifier}! {e}")

    def _format_date_range(self, key: str, item: Dict) -> None:
        if item.get(key):
            time = item.get(key)
            now = datetime.now()
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