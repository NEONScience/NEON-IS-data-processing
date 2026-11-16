# Dockerfile for NEON IS Data Processing - Belfort (aepg600m) Preciptation combine module
# Build with the following command
# docker build --no-cache -t neon-is-precip-aepg-comb-r <path to this directory>

# Start with the neon-is-precip-aepg-comb-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v2.2.0

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.precip.aepg.comb"

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org
MAINTAINER "Teresa Burlingame" tburlingame@battelleecology.org

# For rclone
ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.62.2

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        wget && \
    rm -rf /var/lib/apt/lists/*

# Multi-platform package install for rclone
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; \
    elif [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then ARCHITECTURE=arm-v7; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; \
    else ARCHITECTURE=amd64; fi && \
    TEMP_DEB="$(mktemp)" && \
    wget -O "$TEMP_DEB" "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}.deb" &&\
    dpkg -i "$TEMP_DEB" &&\
    rm -f "$TEMP_DEB"
    
# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in calibration workflow
COPY ${FLOW_DIR}/${APP_DIR}/wrap.precip.aepg.comb.R .
COPY ${FLOW_DIR}/${APP_DIR}/flow.precip.aepg.comb.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
