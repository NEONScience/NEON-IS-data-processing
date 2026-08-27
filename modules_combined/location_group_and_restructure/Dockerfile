# Dockerfile for NEON IS Data Processing - Group in location metadata and restructure+merge data files by location
# This image combines four modules: filter-joiner, kafka combiner, flow.loc.repo.strc, flow.loc.data.trnc.comb,
# and flow.loc.grp.asgn
# Build with the following command
# docker build --no-cache -t neon-is-loc-grp-strc-comb -f <path to this file> <path to parent directory of flow/ and modules/>

# Start with the base R image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v2.1.0

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Build in the filter-joiner python module
ARG MODULE_DIR="modules"
ARG APP_DIR="filter_joiner"
ARG APP_DIR_2="errored_file_manifest"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt

 
RUN apt update && \
    apt-get install -y --no-install-recommends \
        git \
        jq \
        openssh-client \
        build-essential \
        ca-certificates \
        wget \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        liblzma-dev \
        xz-utils && \
    rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.11.9
RUN wget -q https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j"$(nproc)" && \
    make altinstall && \
    cd / && \
    rm -rf /usr/src/app/Python-${PYTHON_VERSION} /usr/src/app/Python-${PYTHON_VERSION}.tgz

RUN python3.11 -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser 


# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}
COPY ${MODULE_DIR}/${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}

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
COPY ./flow/flow.loc.grp.asgn/renv.lock ./renv.lock.loc.grp.asgn
RUN R -e 'renv::restore(lockfile="./renv.lock.loc.grp.asgn")'
COPY ./${MODULE_DIR}/flow.loc.repo.strc/renv.lock ./renv.lock.repo.strc
RUN R -e 'renv::restore(lockfile="./renv.lock.repo.strc")'
COPY ./${MODULE_DIR}/flow.loc.data.trnc.comb/renv.lock ./renv.lock.comb.trnc
RUN R -e 'renv::restore(lockfile="./renv.lock.comb.trnc")'


# Copy in application code
COPY ./${MODULE_DIR}/flow.loc.repo.strc/wrap.loc.repo.strc.R .
COPY ./${MODULE_DIR}/flow.loc.repo.strc/flow.loc.repo.strc.R .
COPY ./${MODULE_DIR}/flow.loc.data.trnc.comb/wrap.loc.data.trnc.comb.R .
COPY ./${MODULE_DIR}/flow.loc.data.trnc.comb/flow.loc.data.trnc.comb.R .
COPY ./${MODULE_DIR}/flow.loc.grp.asgn/wrap.loc.grp.asgn.R .
COPY ./${MODULE_DIR}/flow.loc.grp.asgn/flow.loc.grp.asgn.R .

# Run as app user
USER appuser

