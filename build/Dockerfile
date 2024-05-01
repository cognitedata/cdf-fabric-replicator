FROM --platform=linux/amd64 python:3.10-slim as builder

RUN set -ex && pip install --upgrade pip && pip install poetry
ENV PATH /root/.local/bin:$PATH

RUN apt update

RUN mkdir -p cdf_fabric_replicator 
WORKDIR cdf_fabric_replicator

COPY pyproject.toml ./
COPY poetry.lock ./
COPY cdf_fabric_replicator cdf_fabric_replicator

RUN mkdir -p /config
COPY build/config_remote.yaml /config/config_remote.yaml

WORKDIR cdf_fabric_replicator
RUN poetry config virtualenvs.create false
RUN poetry install --without dev

FROM builder as base
COPY cdf_fabric_replicator cdf_fabric_replicator

WORKDIR cdf_fabric_replicator
CMD ["python", "-m", "cdf_fabric_replicator", "/config/config_remote.yaml"]