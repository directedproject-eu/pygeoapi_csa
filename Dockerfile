FROM python:3.11-alpine as base

LABEL maintainer="Jan Speckamp <j.speckamp@52north.org>" \
      org.opencontainers.image.authors="Jan Speckamp <j.speckamp@52north.org>" \
      org.opencontainers.image.url="https://github.com/52North/connected-systems-pygeoapi" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.vendor="52°North GmbH" \
      org.opencontainers.image.licenses="Apache 2.0" \
      org.opencontainers.image.ref.name="52north/connected-systems-pygeoapi" \
      org.opencontainers.image.title="52°North OGC API Connected Systems" \
      org.opencontainers.image.description="Implementation of OGC API Connected Systems"


# alpine is confused where to look for python libraries so we need to support it here
ENV PYTHONPATH /usr/lib/python3.11/site-packages
ENV PROJ_DIR=/usr
ENV PYTHONUNBUFFERED=1

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
COPY docker/examples/hybrid-csa/openapi-config-csa.yml .
COPY docker/examples/hybrid-csa/pygeoapi-config.yml .
COPY hypercorn.conf.py .

WORKDIR /app/connected-systems-api
CMD ["sh", "-c", "python setup.py && hypercorn -c ../hypercorn.conf.py app:APP"]

FROM base as toardb
# individual requirements for toardb-provider
COPY requirements_toardb_csa.txt .
RUN pip install -r requirements_toardb_csa.txt

FROM base as hybrid
# individual requirements for hybrid-provider
COPY requirements_hybrid_csa.txt .
RUN pip install -r requirements_hybrid_csa.txt