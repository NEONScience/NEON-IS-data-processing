####
#
# This dockerfile will build the combined module for computing troll-specific flags and computing conductivity.
# Example command (must be run from project root directory to include flow and modules_combined path in Docker context):
# docker build -t neon-is-troll-flags-cond-conv-r -f ./modules_combined/troll_flags_and_cond_conv/Dockerfile .
#
###
# Start with the base R image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.2.1

ARG MODULE_DIR="flow"
ARG CONTAINER_APP_DIR="/usr/src/app"

WORKDIR ${CONTAINER_APP_DIR}

# Create app user and data directory
RUN groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser 
    
# Copy in application code
COPY ./${MODULE_DIR}/flow.troll.flags/flow.troll.flags.R .
COPY ./${MODULE_DIR}/flow.troll.cond.conv/flow.troll.cond.conv.R .
COPY ./${MODULE_DIR}/flow.troll.cond.conv/wrap.troll.cond.conv.R .

# Run as app user
USER appuser

