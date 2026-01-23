# Dockerfile for NEON IS Data Processing - sunav2 uncertainty module combined with filter-joiner and insufficient data

# Start with the cal package image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-cal-r:v3.1.0

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Add in the python-based filter-joiner module
ARG MODULE_DIR="modules"
ARG APP_DIR="filter_joiner"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt


RUN apt update && \
    apt-get install -y --no-install-recommends \
        python3.8 && \
    apt install -y python3-pip && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
   apt-get autoremove -y && \
	 apt-get autoclean -y && \
	 rm -rf /var/lib/apt/lists/* && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}


# Build in the uncertainty module
ARG MODULE_DIR="flow"
ARG APP_DIR_1="flow.sunav2.exp.uncert"
ARG APP_DIR_2="flow.insufficient.data"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./flow/flow.sunav2.exp.uncert/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in R code
COPY ./flow/flow.sunav2.exp.uncert/flow.sunav2.exp.uncert.R .
COPY ./flow/flow.sunav2.exp.uncert/wrap.sunav2.exp.uncert.R .

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./flow/flow.insufficient.data/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in R code
COPY ./flow/flow.insufficient.data/flow.insufficient.data.R .
COPY ./flow/flow.insufficient.data/wrap.insufficient.data.R .

# Run as app user
USER appuser
