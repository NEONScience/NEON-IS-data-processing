# Dockerfile for NEON IS Data Processing - Subsurface tchain file Processing. Combines leveltroll400 and hobou24 scripts
# Example command (must be run from project root directory to include both modules in Docker context):
# Build with the following command
# docker build --no-cache -t neon-is-lt400-hu24-files -f ./modules_combined/pluvio_flags_comb/Dockerfile .

# Start with the neon-is-base-r image.
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.0

ARG MODULE_DIR="flow"
ARG APP_DIR_1="flow.hobou24.files"
ARG APP_DIR_2="flow.leveltroll400.files"

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${MODULE_DIR}/${APP_DIR_1}/renv.lock /renv.lock.1
COPY ${MODULE_DIR}/${APP_DIR_2}/renv.lock /renv.lock.2
RUN R -e 'renv::restore(lockfile="/renv.lock.1")'
RUN R -e 'renv::restore(lockfile="/renv.lock.2")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in subsurface file workflow
COPY ./${MODULE_DIR}/${APP_DIR_1}/flow.hobou24.files.R .
COPY ./${MODULE_DIR}/${APP_DIR_1}/wrap.subs.hobou24.files.R .
COPY ./${MODULE_DIR}/${APP_DIR_2}/flow.leveltroll400.files.R .
COPY ./${MODULE_DIR}/${APP_DIR_2}/wrap.subs.leveltroll400.files.R .

# Run as app user
USER appuser
