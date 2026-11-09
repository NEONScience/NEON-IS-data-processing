# Dockerfile for NEON IS Data Processing - Select thresholds by date, source-id, term and context, plus process tipping bucket precipitation
# Example command (must be run from project root directory to include both modules in Docker context):
# Build with the following command
# docker build --no-cache -t neon-is-thsh-slct-prcp-stat -f ./modules_combined/threshold_select_precip_stat/Dockerfile .

# Start with the neon-is-pack-qaqc-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.15

# Build in the threshold selection module
ARG MODULE_DIR="flow"
ARG APP_DIR="flow.thsh.slct"

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
COPY ./${MODULE_DIR}/${APP_DIR}/renv.lock ./renv.lock
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in application code
COPY ./${MODULE_DIR}/${APP_DIR}/flow.thsh.slct.R .
COPY ./${MODULE_DIR}/${APP_DIR}/wrap.thsh.slct.R .

# Load precip processing module. 
# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./flow/flow.precip.bucket/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in R code
COPY ./${MODULE_DIR}/flow.precip.bucket/flow.precip.bucket.R .
COPY ./${MODULE_DIR}/flow.precip.bucket/wrap.precip.bucket.R .

# Run as app user
USER appuser
