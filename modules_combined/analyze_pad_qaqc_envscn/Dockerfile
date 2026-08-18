# Dockerfile for NEON IS Data Processing - Combined padded_timeseries_analyzer and Basic QA/QC module (plausibility) and enviroscan custom flagging
# Example command (must be run from project root directory to include both modules in Docker context):
# Build with the following command
# docker build --no-cache -t neon-is-qaqc-plau-envscn -f ./modules_combined/analyze_pad_qaqc_envscn/Dockerfile .

# Start with the neon-is-pack-qaqc-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.14

ARG MODULE_DIR="modules"
ARG APP_DIR_1="padded_timeseries_analyzer"
ARG APP_DIR_2="filter_joiner"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR_1}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt

 
RUN apt update && \
    apt-get install -y --no-install-recommends \
        jq \
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
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt && \
    apt-get autoremove -y && \
    apt-get autoclean -y

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR_1} ${CONTAINER_APP_DIR}/${APP_DIR_1}
COPY ${MODULE_DIR}/${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the threshold selection module
ARG MODULE_DIR="flow"
ARG APP_DIR="flow.qaqc.plau"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./${MODULE_DIR}/${APP_DIR}/renv.lock ./renv.lock
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in application code
COPY ./${MODULE_DIR}/${APP_DIR}/wrap.qaqc.plau.R .
COPY ./${MODULE_DIR}/${APP_DIR}/flow.qaqc.plau.R .

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Build in the custom enviroscan flags
ARG APP_DIR2="flow.envscn.temp.flags"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./${MODULE_DIR}/${APP_DIR2}/renv.lock ./renv.lock2
RUN R -e 'renv::restore(lockfile="./renv.lock2")'

# Copy in application code
COPY ./${MODULE_DIR}/${APP_DIR2}/wrap.envscn.temp.flags.R .
COPY ./${MODULE_DIR}/${APP_DIR2}/flow.envscn.temp.flags.R .
COPY ./${MODULE_DIR}/${APP_DIR2}/def.apply.temp.flags.R .
COPY ./${MODULE_DIR}/${APP_DIR2}/def.calc.temp.flags.R .
COPY ./${MODULE_DIR}/${APP_DIR2}/def.find.temp.sensor.R .
COPY ./${MODULE_DIR}/${APP_DIR2}/def.load.temp.sensors.R .
COPY ./${MODULE_DIR}/${APP_DIR2}/def.sort.qf.cols.R .
# Run as app user
USER appuser
