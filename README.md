# connected-systems-pygeoapi

Proof of Concept of the OGC Connected Systems API based on pygeoapi

## Installation


### Docker
Example Setups for each backend are provided in the respective subfolder in the `docker` subdirectory.

```commandline


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

```

