# Dockerfile for NEON IS Data Processing - Statistics package
# Example command (run from project parent directory to include pack/ path in Docker context):
# docker build --no-cache -t neon-is-pack-stat-r -f ./pack/NEONprocIS.stat/Dockerfile .

# Start with the neon-is-base-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.1

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# copy the NEONprocIS.qaqc package from local
COPY ./pack/NEONprocIS.stat/ /pack/NEONprocIS.stat/

# Install devtools & renv (package manager), restore from specific versions of dependencies, install NEONprocIS.base from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.stat/renv.lock")' && \
    R -e 'renv::install("/pack/NEONprocIS.stat")' && \
    rm -rf /pack


