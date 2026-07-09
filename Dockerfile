# Dockerfile for running dlt pipelines

# Dockerfile is based heavily on the example uv dockerfile:
# https://github.com/astral-sh/uv-docker-example

# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

ARG QSV_VERSION="20.1.0"
ARG XML_FILE_SPLITTER_VERSION="v0.1.2"
ARG XSV_VALIDATOR_VERSION="v2026-07"

# Set environment variable to noninteractive to prevent prompts during apt operations
ENV DEBIAN_FRONTEND=noninteractive

# add tini and git
RUN apt-get update -y && apt-get install -y --no-install-recommends tini git ca-certificates wget unzip libwayland-client0 && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

# download and install the xml_file_splitter, xsv-validator, and qsv binaries, and copy them to /usr/local/bin
RUN ARCH=$(uname -m) && \
    wget https://github.com/ialarmedalien/xml_file_splitter/releases/download/${XML_FILE_SPLITTER_VERSION}/xml_file_splitter-${ARCH}-unknown-linux-gnu.tar.gz && \
    tar -xvf xml_file_splitter-${ARCH}-unknown-linux-gnu.tar.gz && \
    mv xml_file_splitter-${ARCH}-unknown-linux-gnu/xml_file_splitter /usr/local/bin/ && \
    # xsv-validator, only need the script
    wget https://github.com/cohere-llc/xsv-validator/archive/refs/tags/${XSV_VALIDATOR_VERSION}.tar.gz && \
    mkdir /tmp/xsv-validator && tar -xvf ${XSV_VALIDATOR_VERSION}.tar.gz --strip-components=1 -C /tmp/xsv-validator/ && \
    mv /tmp/xsv-validator/xsv-validate.sh /usr/local/bin/ && \
    chmod +x /usr/local/bin/xsv-validate.sh && \
    rm -fr /tmp/* && \
    # qsv release -- only need the `qsv` binary from it
    wget https://github.com/dathere/qsv/releases/download/${QSV_VERSION}/qsv-${QSV_VERSION}-${ARCH}-unknown-linux-gnu.zip && \
    unzip qsv-${QSV_VERSION}-${ARCH}-unknown-linux-gnu.zip -d /tmp/qsv && \
    mv /tmp/qsv/qsv /usr/local/bin/ && \
    rm -rf /tmp/*

# check install of xsv-validate
RUN xsv-validate.sh --help

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Omit development dependencies
# ENV UV_NO_DEV=1

# don't try to synchronise each time uv is executed
ENV UV_NO_SYNC=1

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Install the project into `/app`
WORKDIR /app

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY --chown=nonroot:nonroot .dlt /app/.dlt
COPY --chown=nonroot:nonroot docs /app/docs
COPY --chown=nonroot:nonroot scripts /app/scripts
COPY --chown=nonroot:nonroot src /app/src
COPY --chown=nonroot:nonroot tests /app/tests
COPY --chown=nonroot:nonroot README.md /app/README.md
COPY --chown=nonroot:nonroot pyproject.toml /app/pyproject.toml
COPY --chown=nonroot:nonroot uv.lock /app/uv.lock

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

RUN chmod +x ./scripts/entrypoint.sh

# make sure that the nonroot user owns the app directory
RUN chown nonroot:nonroot /app

# Use the non-root user to run our application
USER nonroot
ENTRYPOINT ["./scripts/entrypoint.sh"]
