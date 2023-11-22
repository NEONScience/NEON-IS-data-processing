# Dockerfile for NEON IS Data Processing - Below Zero Pressure flags

# Start with the neon-is-base-r image.
FROM quay.io/battelleecology/neon-is-base-r:v1.4.2

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

# Copy in troll flag workflow
COPY ./flow.troll.flags.R .
COPY ./wrap.troll.flags.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
