from pygeoapi.flask_app import CONFIG, OPENAPI
from api import *

PLUGINS["provider"]["ElasticSearchConnectedSystems"] = \
    "provider.part1.elasticsearch_csa.ConnectedSystemsESProvider"
PLUGINS["provider"]["TimescaleDBConnectedSystems"] = \
    "provider.part2.timescaledb_csa.ConnectedSystemsTimescaleDBProvider"

csapi_ = CSAPI(CONFIG, OPENAPI)


async def setup_db():
    """ Initialize peristent database/provider connections """
    if csapi_.csa_provider_part1:
        await csapi_.csa_provider_part1.open()
        await csapi_.csa_provider_part1.setup()
    if csapi_.csa_provider_part2:
        await csapi_.csa_provider_part2.open()
        await csapi_.csa_provider_part2.setup()


async def close_db():
    """ Clean exit database/provider connections """
    if csapi_.csa_provider_part1:
        await csapi_.csa_provider_part1.close()
    if csapi_.csa_provider_part2:
        await csapi_.csa_provider_part2.close()


async def main():
    await setup_db()
    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
