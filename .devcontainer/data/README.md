# Data

Add test data into this directory.
The contents of this directory will be mounted to the `connected-systems-api` service image to container directory `/srv/data/`.
These files can be used as static resources and must be configured in the file [`../connected-systems-api/pygeoapi-config.yml`](../connected-systems-api/pygeoapi-config.yml).
The [supported types](https://docs.pygeoapi.io/en/latest/data-publishing/index.html) are documented.
