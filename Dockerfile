FROM python:3.11-alpine as base
LABEL maintainer="Jan Speckamp <j.speckamp@52north.org>"

ENV PYTHONPATH /usr/lib/python3.11/site-packages
ENV PROJ_DIR=/usr

RUN apk update
RUN apk add gcc musl-dev git proj proj-dev proj-util py3-numpy py3-shapely py3-shapely-pyc

WORKDIR /app

# Install requirements
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY requirements_nodeps.txt .
RUN pip install --no-deps -r requirements_nodeps.txt

# copy application files
COPY connected-systems-api connected-systems-api
COPY docker/examples/elasticsearch-csa/openapi-config-csa.yml .
COPY docker/examples/elasticsearch-csa/pygeoapi-config.yml .
COPY gunicorn.conf.py .

ENV PYTHONUNBUFFERED=1

WORKDIR /app/connected-systems-api
CMD ["gunicorn", "-c", "../gunicorn.conf.py", "flask_app:APP"]


FROM base as toardb
# individual requirements for toardb-provider
COPY requirements_toardb_csa.txt .
RUN pip install -r requirements_toardb_csa.txt

FROM base as elasticsearch
# individual requirements for elasticsearch-provider
COPY requirements_elasticsearch_csa.txt .
RUN pip install -r requirements_elasticsearch_csa.txt