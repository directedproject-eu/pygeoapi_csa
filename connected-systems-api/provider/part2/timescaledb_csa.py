import json
import logging
import uuid
from typing import List, Dict, Tuple

import asyncpg
from asyncpg import Connection

from elastic_transport import NodeConfig
from elasticsearch import AsyncElasticsearch
from elasticsearch_dsl import Search
from pygeoapi.provider.base import ProviderConnectionError, ProviderGenericError, ProviderItemNotFoundError

from .util import TimescaleDbConfig, ObservationQuery, Observation
from ..definitions import ConnectedSystemsPart2Provider, CSAGetResponse, DatastreamsParams, ObservationsParams, \
    CSACrudResponse, ResulttimePhenomenontimeParam
from ..connector_elastic import create_many, connect_elasticsearch, ElasticSearchConfig, search, parse_datetime_params, \
    parse_csa_params, parse_temporal_filters

from .formats.om_json_scalar import OMJsonSchemaParser

LOGGER = logging.getLogger(__name__)


class ConnectedSystemsTimescaleDBProvider(ConnectedSystemsPart2Provider):
    _pool: asyncpg.connection = None
    _es: AsyncElasticsearch = None
    datastreams_index_name = "datastreams"

    # TODO: check if there are further problematic fields
    datastream_mappings = {
        "properties": {
            "system": {
                "type": "keyword"
            },
            "id": {
                "type": "keyword"
            }
        }
    }

    def __init__(self, provider_def):
        super().__init__(provider_def)
        self._ts_config = TimescaleDbConfig(
            hostname=provider_def["timescale"]["host"],
            port=provider_def["timescale"]["port"],
            user=provider_def["timescale"]["user"],
            password=provider_def["timescale"]["password"],
            dbname=provider_def["timescale"]["dbname"],
        )

        self._es_config = ElasticSearchConfig(
            hostname=provider_def["elastic"]["host"],
            port=provider_def["elastic"]["port"],
            user=provider_def["elastic"]["user"],
            password=provider_def["elastic"]["password"],
            dbname=provider_def["elastic"]["dbname"],
        )
        self.parser = OMJsonSchemaParser()

    async def open(self):
        await self.open_timescaledb()
        await self.open_elasticsearch()

    async def open_timescaledb(self):
        self._pool = await asyncpg.create_pool(self._ts_config.connection_string(),
                                               min_size=self._ts_config.pool_min_size,
                                               max_size=self._ts_config.pool_max_size)

        statements = []
        if self._ts_config.drop_tables:
            statements.append("""DROP TABLE IF EXISTS observations;""")

        statements.append("""CREATE TABLE IF NOT EXISTS observations (
                                                        uuid UUID DEFAULT gen_random_uuid(),
                                                        phenomenontime TIMESTAMPTZ ,
                                                        resulttime TIMESTAMPTZ NOT NULL,
                                                        result JSONB,
                                                        geom GEOMETRY,
                                                        foi text,
                                                        datastream text,
                                                        observedproperty text
                                                    );
                                                    """)

        statements.append("""SELECT create_hypertable(
                                'observations',
                                by_range('resulttime'),
                                if_not_exists => TRUE
                            );
                            """)

        connection: Connection
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                for stmnt in statements:
                    await connection.execute(stmnt)
        return self

    async def open_elasticsearch(self):
        self._es: AsyncElasticsearch = await connect_elasticsearch(
            self._es_config,
            [
                (self.datastreams_index_name, self.datastream_mappings),
            ])

    async def close(self):
        await self._pool.close()
        await self._es.close()

    def get_conformance(self) -> List[str]:
        """Returns the list of conformance classes that are implemented by this provider"""
        return []

    async def create(self, type: str, items: List[Dict]) -> CSACrudResponse:
        """
        Create a new item

        :param item: `dict` of new item

        :returns: identifier of created item
        """

        routines: List[Tuple[str, Dict]] = [None] * len(items)
        if type == "datastream":
            # check if linked system exists
            system_exists = await self._es.exists(index="systems", id=items[0]["system"])
            if not system_exists:
                raise ProviderItemNotFoundError(f"no system with id {items[0]['system']} found!")

            # create in elasticsearch
            for i, item in enumerate(items):
                if "id" not in item:
                    # We may have to generate id as it is not always required
                    identifier = str(uuid.uuid4())
                    item["id"] = identifier
                else:
                    identifier = item["id"]

                routines[i] = (identifier, item)
                return await create_many(self._es, self.datastreams_index_name, routines)
        elif type == "observation":
            # check if linked datastream exists
            datastream_id = items[0]['datastream']
            datastream_exists = await self._es.exists(index=self.datastreams_index_name, id=datastream_id)
            if not datastream_exists:
                raise ProviderItemNotFoundError(f"no datastream with id {datastream_id} found!")

            # create in timescaledb
            # TODO: resolve to different parsers based on something?
            return await self.put_observations([self.parser.decode(datastream_id, elem) for elem in items])
        else:
            raise ProviderGenericError(f"unrecognized type: {type}")

    async def put_observations(self, observations: List[Observation]) -> List[str]:
        res = [""] * len(observations)
        connection: Connection
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                # reformat to tuple
                for idx, obs in enumerate(observations):
                    # TODO: use prepared statement
                    res[idx] = str(await connection.fetchval(
                        "INSERT INTO observations (phenomenontime, resulttime, result, geom, foi, datastream, observedproperty) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING uuid;",
                        obs.phenomenonTime,
                        obs.resultTime,
                        obs.result,
                        obs.geom,
                        obs.foi,
                        obs.datastream,
                        obs.observedProperty
                    ))
        return res

    async def update(self, identifier, item):
        """
        Updates an existing item

        :param identifier: feature id
        :param item: `dict` of partial or full item

        :returns: `bool` of update result
        """

        raise NotImplementedError()

    async def delete(self, identifier):
        """
        Deletes an existing item

        :param identifier: item id

        :returns: `bool` of deletion result
        """

        raise NotImplementedError()

    async def query_datastreams(self, parameters: DatastreamsParams) -> CSAGetResponse:
        """
        implements queries on datastreams as specified in openapi-connectedsystems-2

        :returns: dict of formatted properties
        """

        query = Search(using=self._es, index=self.datastreams_index_name)
        query = parse_csa_params(query, parameters)
        query = parse_temporal_filters(query, parameters)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        if parameters.schema:
            response = await search(self._es, self.datastreams_index_name, query.to_dict(), parameters)
            return list(map(lambda x: x["schema"], response[0])), []
        else:
            return await search(self._es, self.datastreams_index_name, query.to_dict(), parameters)

    async def query_observations(self, parameters: ObservationsParams) -> CSAGetResponse:
        """
        implements queries on observations as specified in openapi-connectedsystems-2

        :returns: dict of formatted properties
        """
        q = ObservationQuery()
        q.with_limit(parameters.limit)

        if parameters.id:
            q.with_id(parameters.id)
        if parameters.offset:
            q.with_offset(parameters.offset)
        if parameters.phenomenontime:
            q.with_time("phenomenontime", parameters.phenomenontime)
        if parameters.resulttime:
            q.with_time("resulttime", parameters.resulttime)
        if parameters.system:
            q.with_system(parameters.system)
        if parameters.foi:
            q.with_foi(parameters.foi)
        if parameters.observedProperty:
            q.with_observedproperty(parameters.observedProperty)

        LOGGER.critical(q.to_sql())

        connection: Connection
        async with self._pool.acquire() as connection:
            print("SELECT * FROM observations " + q.to_sql())
            print(*q.parameters)
            response = await connection.fetch("SELECT * FROM observations " + q.to_sql(), *q.parameters)

            if len(response) > 0:
                return [self.parser.encode(row) for row in response], []
            else:
                # check if this query returns 404 or 200 with empty body in case of no return
                if parameters.id:
                    return None
                else:
                    return [], []
