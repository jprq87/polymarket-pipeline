FROM ubuntu:24.04

RUN apt-get update && apt-get install -y python3.12 git curl && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN curl -LsSf https://getbruin.com/install/cli | sh

ENV PATH="/root/.bruin:/root/.local/bin:$PATH"

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

RUN git init

ENV PATH="/app/.venv/bin:$PATH"

# /app/logs is intended to be mounted as a volume at run time
# /app/.bruin.yml and /app/bruin_gcp.json are also mounted at run time

ENTRYPOINT ["bruin", "run", "./polymarket-pipeline"]
CMD ["--environment", "dev"]