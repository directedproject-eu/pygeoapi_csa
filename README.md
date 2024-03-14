# connected-systems-pygeoapi

Proof of Concept of the OGC Connected Systems API based on pygeoapi

## Installation

### Docker
Example Setups for each backend are provided in the respective subfolder in the `docker` subdirectory.

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

# Usage

The API is accessible at `<host>:5000` and provides a HTML landing page for easy navigation.

# License

The software is licensed under the `Apache 2.0 License`. See [LICENSE.md](LICENSE.md) for details.

# Contributors

