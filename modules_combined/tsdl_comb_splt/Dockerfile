# Dockerfile for NEON IS Data Processing - Combined filter-joiner and tsdl_comb_splt (Combine Temperature Specific Depth Lakes data and split by HOR.VER)
# Example command (must be run from project root directory to include both modules in Docker context):
# Build with the following command
# docker build --no-cache -t neon-is-tsdl-comb-splt -f ./modules_combined/tsdl_comb_splt/Dockerfile .

# Start with the base image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.0

# Add in the python-based filter-joiner module
ARG MODULE_DIR="modules"
ARG APP_DIR="filter_joiner"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG FLOW_DIR="./flow"
ARG FLOW_APP_DIR="flow.tsdl.comb.splt"
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

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in application code
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/flow.tsdl.comb.splt.R .
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/wrap.file.comb.tsdl.splt.R .
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/wrap.schm.map.char.gsub.R .
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/def.map.char.gsub.R .
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/def.schm.avro.pars.map.R .
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/def.find.mtch.str.best.R .


# Run as app user
USER appuser
