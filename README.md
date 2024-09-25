# connected-systems-pygeoapi

Proof of Concept of the OGC Connected Systems API based on pygeoapi

## Installation

### Docker

Example Setups for each backend are provided in the respective subfolder in the `./docker/` subdirectory.

Build appropriate docker container (choose either target)

```commandline
docker compose build connected-systems-api
```

Note: When building manually make sure to specify the `target` as either `elasticsearch` or `toardb`.

```commandline
docker build --target=<elasticsearch|toardb> .
```

### Local/Development Installation

The specific installation instructions depend on the actual backend to be used, as each backend may require additional dependencies.

Installation of requirements:

```commandline
pip install -r requirements.txt
pip install -r --no-deps requirements_nodeps.txt

[if toardb backend is used]
pip install -r requirements_toardb_csa.txt

[if elasticsearch backend is used]
pip install -r requirements_elasticsearch_csa.txt
```

The application can then be started via

```commandline
PYGEOAPI_CONFIG=<path-to-pygeoapi-config.yml> \
PYGEOAPI_OPENAPI=<path-to-openapi-config-csa.yml> \
python3 connected-systems-api/flask_app.py
```

### devcontainer

This repository contains [devcontainer](https://code.visualstudio.com/docs/devcontainers/containers) configurations.
Before using them, the `docker/examples/hybrid-csa/.env-sample` or any other working `.env` MUST be provided by copying it to the `.devcontainer/` folder.

Remember to rebuild the containers, if any other example set-up from `docker/examples` was executed beforehand.

### Example Data

You can insert example data into your running instance (`url_stub`) by using the [simulator](./tools/simulator/simulator.py).
Ensure to set-up your python environment accordingly and install the [required dependencies](./tools/simulator/requirements.txt) in your simulator env.
You can limit the amount of observations (`num_of_obs_to_insert`) being inserted in the `simlutor.py`

## Usage

The API is accessible at `<host>:5000` and provides a HTML landing page for easy navigation.

### Configuration

The default configuration is done by the two configuration files:

* `openapi-config-csa.yml`
* `pygeoapi-config.yml`

The providers implementing part 1 and 2 of the specification are currently using [timescaledb]() and [elastic search]().
The configuration of these backend services can be achieved by providing the according values in the `pygeoapi-config.yml` or via environment variables as outlined in the following list.
The values provided via environment variables superseed the `pygeoapi-config.yml` values.
The current implementation allows only the use of **one** elastic search cluster, when providing the configuration via environment variables.

* **Elastic Search Cluster**
  * `ELASTIC_HOST`
  * `ELASTIC_PORT`
  * `ELASTIC_DB`
  * `ELASTIC_USER`
  * `ELASTIC_PASSWORD`

* **TimescaleDB**
  * `TIMESCALEDB_HOST`
  * `TIMESCALEDB_PORT`
  * `TIMESCALEDB_DB`
  * `TIMESCALEDB_USER`
  * `TIMESCALEDB_PASSWORD`

## License

The software is licensed under the `Apache 2.0 License`. See [LICENSE.md](LICENSE.md) for details.

## Contributors
