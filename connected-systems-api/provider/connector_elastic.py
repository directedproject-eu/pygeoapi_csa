import asyncio
import logging
from typing import Coroutine, Any, Union

from elasticsearch import AsyncElasticsearch
from elasticsearch_dsl import Search
from elastic_transport import NodeConfig

from .definitions import *
from pygeoapi.provider.base import ProviderConnectionError, ProviderInvalidDataError, ProviderQueryError

LOGGER = logging.getLogger(__name__)


def parse_datetime_params(query: Search, parameters: DatetimeParam) -> Search:
    # Parse dateTime filter
    if parameters.datetime_start() and parameters.datetime_end():
        query = query.filter("range", validTime_parsed={"gte": parameters.datetime_start().isoformat(),
                                                        "lte": parameters.datetime_end().isoformat()})
    if parameters.datetime_start():
        query = query.filter("range", validTime_parsed={"gte": parameters.datetime_start().isoformat()})
    if parameters.datetime_end():
        query = query.filter("range", validTime_parsed={"lte": parameters.datetime_end().isoformat()})
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


def parse_temporal_filters(query, parameters: ResulttimePhenomenontimeParam) -> Search:
    # Parse resultTime filter
    if parameters.resulttime_start() and parameters.resulttime_end():
        query = query.filter("range", validTime_parsed={"gte": parameters.resulttime_start().isoformat(),
                                                        "lte": parameters.resulttime_end().isoformat()})
    if parameters.resulttime_start():
        query = query.filter("range", validTime_parsed={"gte": parameters.resulttime_start().isoformat()})
    if parameters.resulttime_end():
        query = query.filter("range", validTime_parsed={"lte": parameters.resulttime_end().isoformat()})

    # Parse phenomenonTime filter
    if parameters.phenomenontime_start() and parameters.phenomenontime_end():
        query = query.filter("range", validTime_parsed={"gte": parameters.phenomenontime_start().isoformat(),
                                                        "lte": parameters.phenomenontime_end().isoformat()})
    if parameters.phenomenontime_start():
        query = query.filter("range", validTime_parsed={"gte": parameters.phenomenontime_start().isoformat()})
    if parameters.phenomenontime_end():
        query = query.filter("range", validTime_parsed={"lte": parameters.phenomenontime_end().isoformat()})

    return query


@dataclass(frozen=True)
class ElasticSearchConfig:
    hostname: str
    port: int
    user: str
    password: str
    dbname: str


async def connect_elasticsearch(config: ElasticSearchConfig) -> AsyncElasticsearch:
    LOGGER.debug(f'Connecting to Elasticsearch at: https://{config.hostname}:{config.port}/{config.dbname}')
    es: AsyncElasticsearch = AsyncElasticsearch(
        [
            NodeConfig(
                scheme="https",
                host=config.hostname,
                port=config.port,
                verify_certs=False,
                ca_certs=None,
                ssl_show_warn=False,
            )
        ],
        http_auth=(config.user, config.password),
        verify_certs=False)
    if not await es.ping():
        msg = f'Cannot connect to Elasticsearch'
        LOGGER.critical(msg)
        raise ProviderConnectionError(msg)

    LOGGER.debug('Determining ES version')
    v = await(es.info())
    v = v['version']['number'][:3]
    if float(v) < 8:
        msg = 'only ES 8+ supported'
        LOGGER.critical(msg)
        raise ProviderConnectionError(msg)
    return es


async def setup_elasticsearch(es: AsyncElasticsearch, mappings: List[Tuple[str, Dict]]) -> AsyncElasticsearch:
    try:
        for index in mappings:
            index_name, index_mapping = index

            await es.options(ignore_status=[400, 404]).indices.delete(index=index_name)

            if not await (es.indices.exists(index=index_name)):
                await es.indices.create(
                    index=index_name,
                    mappings=index_mapping
                )
    except Exception as e:
        LOGGER.exception(e)

    LOGGER.debug("finished initializing AsyncElasticsearch")
    return es


async def search(es: AsyncElasticsearch,
                 index: str,
                 body: Dict,
                 parameters: CSAParams,
                 excludes=None) -> CSAGetResponse:
    # Select appropriate strategy here: For collections >10k elements search_after must be used
    if excludes is None:
        excludes = []
    found = (await es.search(body=body,
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


async def create_many(es: AsyncElasticsearch, index: str, items: List[Tuple[str, Dict]]) -> CSACrudResponse:
    routines = [None] * len(items)

    for i, elem in enumerate(items):
        identifier, item = elem
        # add to ES if not already present
        exists = await es.exists(index=index, id=identifier)
        if exists.body:
            # TODO: what should happen here?
            msg = 'record already exists'
            LOGGER.error(msg)
            raise ProviderInvalidDataError(msg)
        else:
            routines[i] = es.index(index=index, id=identifier, document=item)

    # wait for completion
    await asyncio.gather(*routines)

    # TODO: check if we need to validate something here
    return [item[0] for item in items]
