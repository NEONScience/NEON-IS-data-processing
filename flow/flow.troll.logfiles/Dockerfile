# Dockerfile for NEON IS Data Processing - Troll Logfile Processing

# Start with the neon-is-base-r image.
FROM quay.io/battelleecology/neon-is-base-r:v1.4.3-e65941ee

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in troll flag workflow
COPY ./flow.troll.logfiles.R .
COPY ./wrap.troll.logfiles.R .

