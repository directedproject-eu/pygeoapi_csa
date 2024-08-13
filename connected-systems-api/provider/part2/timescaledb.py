import json
import logging
import uuid
from typing import List, Dict, Tuple

import asyncpg
from asyncpg import Connection
from elasticsearch import AsyncElasticsearch
from elasticsearch_dsl import Search, AsyncSearch, async_connections, AsyncDocument, Keyword
from pygeoapi.provider.base import ProviderGenericError, ProviderItemNotFoundError

from .formats.om_json_scalar import OMJsonSchemaParser
from .util import TimescaleDbConfig, ObservationQuery, Observation
from ..connector_elastic import ElasticsearchConnector, ElasticSearchConfig, parse_csa_params, \
    parse_temporal_filters
from ..definitions import *

LOGGER = logging.getLogger(__name__)


class Datastream(AsyncDocument):
    id = Keyword()
    system = Keyword()

    class Index:
        name = "datastreams"


class ConnectedSystemsTimescaleDBProvider(ConnectedSystemsPart2Provider, ElasticsearchConnector):
    _pool: asyncpg.connection = None

    def __init__(self, provider_def):
        super().__init__(provider_def)
        self.base_url = provider_def["base_url"]
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
        self._pool = await asyncpg.create_pool(self._ts_config.connection_string(),
                                               min_size=self._ts_config.pool_min_size,
                                               max_size=self._ts_config.pool_max_size)

        await self.connect_elasticsearch(self._es_config)
        await self.setup()

    async def close(self):
        await self._pool.close()
        es = async_connections.get_connection()
        await es.close()

    async def setup(self):
        ## Setup TimescaleDB
        statements = ["""CREATE EXTENSION IF NOT EXISTS POSTGIS;"""]

        if self._ts_config.drop_tables:
            statements.append("""DROP TABLE IF EXISTS observations;""")

        statements.append("""CREATE TABLE IF NOT EXISTS observations (
                                                                uuid UUID DEFAULT gen_random_uuid(),
                                                                resulttime TIMESTAMPTZ NOT NULL,
                                                                phenomenontime TIMESTAMPTZ,
                                                                datastream_id TEXT NOT NULL,
                                                                result BYTEA NOT NULL,
                                                                sampling_feature_id text,
                                                                procedure_link text,
                                                                parameters text
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

    def get_conformance(self) -> List[str]:
        """Returns the list of conformance classes that are implemented by this provider"""
        LOGGER.error("TODO: define conformance classes")
        return []

    async def create(self, type: EntityType, item: Dict) -> CSACrudResponse:
        """
        Create a new item

        :param item: `dict` of new item

        :returns: identifier of created item
        """
        if type == EntityType.DATASTREAMS:
            # check if linked system exists
            if not self._exists(System.search().filter("term", id=item["system"])):
                raise ProviderItemNotFoundError(f"no system with id {item['system']} found!")

            # create in elasticsearch
            if "id" not in item:
                # We may have to generate id as it is not always required
                identifier = str(uuid.uuid4())
                item["id"] = identifier
            else:
                identifier = item["id"]

            try:
                ds = Datastream(**item)
                await ds.save(refresh=True)
                return identifier
            except Exception as e:
                raise ProviderGenericError(f"error saving datastream {e}")

        elif type == EntityType.OBSERVATIONS:
            # check if linked datastream exists
            datastream_id = item['datastream']
            if not await self._exists(Datastream.search().filter("term", id=item["datastream"])):
                raise ProviderItemNotFoundError(f"no datastream with id {datastream_id} found!")

            # create in timescaledb
            # TODO: resolve to different parsers based on something?
            return await self.put_observations([self.parser.decode(item)])
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
                        "INSERT INTO observations (resulttime, phenomenontime, datastream_id, result, sampling_feature_id, procedure_link, parameters) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING uuid;",
                        obs.resultTime,
                        obs.phenomenonTime,
                        obs.datastream_id,
                        obs.result,
                        obs.sampling_feature_id,
                        obs.procedure_link,
                        obs.parameters
                    ))
        return res

    async def update(self, type: EntityType, identifier: str, item: Dict):
        """
        Updates an existing item

        :param identifier: feature id
        :param item: `dict` of partial or full item

        :returns: `bool` of update result
        """

        raise NotImplementedError()

    async def delete(self, type: EntityType, identifier: str, cascade: bool = False):
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

        query = AsyncSearch(using=self._es, index=self.datastreams_index_name)
        query = parse_csa_params(query, parameters)
        query = parse_temporal_filters(query, parameters)

        if parameters.system is not None:
            query = query.filter("terms", system=parameters.system)

        LOGGER.debug(json.dumps(query.to_dict(), indent=True, default=str))
        if parameters.schema:
            response = await self.search(query, parameters)
            return list(map(lambda x: x["schema"], response[0])), []
        else:
            return await self.search(query, parameters)

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
        if parameters._phenomenonTime:
            q.with_time("phenomenontime", parameters._phenomenonTime)
        if parameters._resultTime:
            q.with_time("resulttime", parameters._resultTime)
        if parameters.foi:
            q.with_foi(parameters.foi)
        if parameters.observedProperty:
            q.with_observedproperty(parameters.observedProperty)
        if parameters.datastream:
            q.with_datastream(parameters.datastream)

        connection: Connection
        async with self._pool.acquire() as connection:
            print("SELECT * FROM observations " + q.to_sql())
            print(*q.parameters)
            response = await connection.fetch("SELECT * FROM observations " + q.to_sql(), *q.parameters)

            if len(response) > 0:
                links = []
                if len(response) == int(parameters.limit):
                    # page is fully filled - we assume a nextpage exists
                    url = self.base_url
                    if parameters.datastream:
                        url += f"/datastreams/{parameters.datastream}"
                    links.append({
                        "title": "next",
                        "rel": "next",
                        "href": parameters.nextlink()
                    })
                return [self.parser.encode(row) for row in response], links
            else:
                # check if this query returns 404 or 200 with empty body in case of no return
                if parameters.id:
                    return None
                else:
                    return [], []
