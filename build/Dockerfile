FROM python:3.11-slim

RUN set -ex && pip install --upgrade pip && pip install poetry pipx
ENV PATH /root/.local/bin:$PATH

RUN apt update
RUN apt install -y curl
RUN apt install -y less
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

RUN mkdir -p cdf_fabric_replicator 
WORKDIR cdf_fabric_replicator

COPY pyproject.toml ./
COPY poetry.lock ./
COPY cdf_fabric_replicator cdf_fabric_replicator
COPY build/entrypoint.sh /
RUN chmod +x /entrypoint.sh

RUN pipx install .

RUN mkdir -p /config
COPY build/config_remote.yaml /config/config_remote.yaml

WORKDIR cdf_fabric_replicator

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["/config/config_remote.yaml"]
