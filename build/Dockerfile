FROM python:3.10-slim as builder

RUN pip install poetry
RUN mkdir -p /cdf_fabric_replicator

COPY pyproject.toml /cdf_fabric_replicator
COPY poetry.lock /cdf_fabric_replicator
COPY cdf_fabric_replicator /cdf_fabric_replicator

WORKDIR /cdf_fabric_replicator
RUN poetry config virtualenvs.in-project true --local
RUN poetry install --without dev

FROM python:3.10-slim as base

COPY --from=builder /cdf_fabric_replicator/ /cdf_fabric_replicator
COPY build/config_remote.yaml /config/config_remote.yaml

# WORKDIR /cdf_fabric_replicator
ENV PATH="/cdf_fabric_replicator/.venv/bin:$PATH"
CMD ["python", "-m", "cdf_fabric_replicator", "/config/config_remote.yaml"]