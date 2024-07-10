# Dockerfile for NEON IS Data Processing - Missing Temp Flag and Conductivity Conversion

# Start with the neon-is-base-r image.
FROM quay.io/battelleecology/neon-is-base-r:v1.4.5-bed3a052

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ./flow.troll.cond.conv.R .
COPY ./wrap.troll.cond.conv.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
