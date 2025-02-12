FROM python:3.13-slim AS builder

RUN pip install uv
RUN mkdir -p /cdf_fabric_replicator

COPY pyproject.toml /cdf_fabric_replicator
COPY uv.lock /cdf_fabric_replicator
COPY cdf_fabric_replicator /cdf_fabric_replicator
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

WORKDIR /cdf_fabric_replicator
RUN uv sync --frozen

FROM python:3.13-slim AS base

COPY --from=builder /cdf_fabric_replicator/ /cdf_fabric_replicator
COPY build/config_remote.yaml /config/config_remote.yaml

# WORKDIR /cdf_fabric_replicator
ENV PATH="/cdf_fabric_replicator/.venv/bin:$PATH"
CMD ["python", "-m", "cdf_fabric_replicator", "/config/config_remote.yaml"]
