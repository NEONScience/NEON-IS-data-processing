# Dockerfile for NEON IS Data Processing - Group in location metadata and restructure+merge data files by location
# This image combines four modules: filter-joiner, kafka combiner, flow.loc.repo.strc, and flow.loc.data.trnc.comb
# Build with the following command
# docker build --no-cache -t neon-is-loc-grp-strc-comb -f <path to this file> <path to parent directory of flow/ and modules/>

# Start with the base R image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.1.3

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Build in the filter-joiner python module
ARG MODULE_DIR="modules"
ARG APP_DIR="filter_joiner"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_DIR="/data"
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
    useradd -r -u 9999 -g appuser appuser && \
    mkdir ${DATA_DIR} && \
    chown appuser:appuser -R ${CONTAINER_APP_DIR} && \
    chown appuser:appuser -R ${DATA_DIR}


# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the restructure_repo_by_location and merge_data_by_location modules
ARG MODULE_DIR="flow"

# Load kafka combiner
# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./${MODULE_DIR}/flow.kfka.comb/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in R code
COPY ./${MODULE_DIR}/flow.kfka.comb/flow.kfka.comb.R .
COPY ./${MODULE_DIR}/flow.kfka.comb/wrap.kfka.comb.R .

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./${MODULE_DIR}/flow.loc.repo.strc/renv.lock ./renv.lock.repo.strc
RUN R -e 'renv::restore(lockfile="./renv.lock.repo.strc")'
COPY ./${MODULE_DIR}/flow.loc.data.trnc.comb/renv.lock ./renv.lock.comb.trnc
RUN R -e 'renv::restore(lockfile="./renv.lock.comb.trnc")'

# Copy in application code
COPY ./${MODULE_DIR}/flow.loc.repo.strc/wrap.loc.repo.strc.R .
COPY ./${MODULE_DIR}/flow.loc.repo.strc/flow.loc.repo.strc.R .
COPY ./${MODULE_DIR}/flow.loc.data.trnc.comb/wrap.loc.data.trnc.comb.R .
COPY ./${MODULE_DIR}/flow.loc.data.trnc.comb/flow.loc.data.trnc.comb.R .

# Run as app user
USER appuser

