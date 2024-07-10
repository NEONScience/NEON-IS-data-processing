# Dockerfile for NEON IS Data Processing - Combined date gap filler and regularization
# This image combines the two modules: date_gap_filler and flow.rglr
# Build with the following command
# docker build --no-cache -t neon-is-gf-rglr -f <path to this file> <path to parent directory of flow/ and modules/>

# Start with the base R image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.4.6-0540506a

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Build in the filter-joiner python module
ARG MODULE_DIR="modules"
ARG APP_DIR="date_gap_filler"
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
	 rm -rf /var/lib/apt/lists/* 


# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the module
ARG MODULE_DIR="flow"

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./${MODULE_DIR}/flow.rglr/renv.lock ./renv.lock
RUN R -e 'renv::restore(lockfile="./renv.lock")' 
#RUN git clone -b deve https://github.com/NEONScience/eddy4R.git 
#RUN R -e 'renv::install("./eddy4R/pack/eddy4R.base",repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' 

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser
WORKDIR /usr/src/app

# Copy in application code
COPY ./${MODULE_DIR}/flow.rglr/wrap.rglr.R .
COPY ./${MODULE_DIR}/flow.rglr/flow.rglr.R .

# Run as app user
USER appuser
