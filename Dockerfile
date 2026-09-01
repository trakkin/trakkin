FROM debian:13-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132 AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential ca-certificates curl git && \
    rm -rf /var/lib/apt/lists/*

ENV MISE_AUTO_INSTALL="0"
ENV MISE_CACHE_DIR="/mise/cache"
ENV MISE_CONFIG_DIR="/mise"
ENV MISE_DATA_DIR="/mise"
ENV MISE_INSTALL_PATH="/usr/local/bin/mise"
ENV MISE_LOCKED="1"
ENV MISE_TRUSTED_CONFIG_PATHS="/app"
ENV MISE_VERSION="2026.8.14"
ENV PATH="/mise/shims:$PATH"

RUN bash -o pipefail -c 'curl -fsSL https://mise.run | sh'

WORKDIR /app

COPY mise.toml mise.lock ./

RUN --mount=type=cache,target=/mise/cache \
    mise install --include-task-tools

COPY frontend/package.json frontend/pnpm-lock.yaml frontend/

RUN --mount=type=cache,target=/pnpm/store \
    mise exec -- pnpm --dir frontend install --frozen-lockfile --prod=false --store-dir /pnpm/store

COPY frontend/ frontend/

RUN mise run build:frontend

COPY Cargo.toml Cargo.lock build.rs ./
COPY migrations/ migrations/
COPY src/ src/

RUN --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/app/target \
    mise run build:backend && \
    install -D -m 755 target/release/trakkin /rootfs/usr/local/bin/trakkin

FROM debian:13-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

ARG TRAKKIN_UID=65532
ARG TRAKKIN_GID=65532

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libgcc-s1 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid "${TRAKKIN_GID}" trakkin && \
    useradd --uid "${TRAKKIN_UID}" --gid "${TRAKKIN_GID}" --home-dir /config --no-create-home --no-log-init --shell /usr/sbin/nologin trakkin && \
    install -d -o "${TRAKKIN_UID}" -g "${TRAKKIN_GID}" -m 0700 /config /run/trakkin/providers

COPY --from=builder /rootfs/ /

ENV HOME=/config
ENV LANG=C.UTF-8
ENV TRAKKIN_BIND=0.0.0.0:8725
ENV TRAKKIN_DATA_DIR=/config
ENV TRAKKIN_PROVIDER_RUNTIME_DIR=/run/trakkin/providers

WORKDIR /config

USER ${TRAKKIN_UID}:${TRAKKIN_GID}

VOLUME ["/config"]

EXPOSE 8725

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD ["trakkin", "healthcheck"]

ENTRYPOINT ["trakkin"]
