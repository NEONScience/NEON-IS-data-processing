# Dockerfile for NEON SAE ECTE L1L4 Data Processing

# start with the base image
FROM quay.io/battelleecology/liangying-devel:deve

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ./flow.sae.ecte.l1l4.R /home/app/flow.sae.ecte.l1l4.R
COPY ./wrap.sae.ecte.l1l4.R /home/app/wrap.sae.ecte.l1l4.R
COPY ./def.loc.meta.R /home/app/def.loc.meta.R

# Run as app user
RUN chown -R app:app /home/app
USER app

