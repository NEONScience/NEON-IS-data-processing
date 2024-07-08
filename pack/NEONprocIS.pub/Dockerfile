# Dockerfile for NEON IS Data Processing - Publication package
# Example command (run from project parent directory to include pack/ path in Docker context):
# docker build --no-cache -t neon-is-pack-pub-r ./pack/NEONprocIS.pub


# Start with the neon-is-base-r image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.4.4-588e4d45

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# copy the NEONprocIS.pub package from local
COPY ./ /pack/NEONprocIS.pub/

# Install devtools & renv (package manager), restore from specific versions of dependencies, install NEONprocIS.pub from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.pub/renv.lock")' && \
    R -e 'renv::install("/pack/NEONprocIS.pub")' && \
    rm -rf /pack


