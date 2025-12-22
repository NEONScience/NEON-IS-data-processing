# Dockerfile for NEON IS Data Processing - Publication package
# Example command (run from project parent directory to include pack/ path in Docker context):
# docker build --no-cache -t neon-is-pack-pub-r -f ./pack/NEONprocIS.pub/Dockerfile .


# Start with the neon-is-base-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.0

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# copy the NEONprocIS.pub package from local
COPY ./pack/NEONprocIS.pub/ /pack/NEONprocIS.pub/

# Install devtools & renv (package manager), restore from specific versions of dependencies, install NEONprocIS.pub from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.pub/renv.lock")' && \
    R -e 'renv::install("/pack/NEONprocIS.pub")' && \
    rm -rf /pack


