# Dockerfile for NEON IS Data Processing - Calibration package

# Start with the neon-is-base-r image. 
FROM quay.io/battelleecology/neon-is-base-r:v0.0.60

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# copy the NEONprocIS.qaqc package from local
COPY ./ /pack/NEONprocIS.stat/

# Install devtools & renv (package manager), restore from specific versions of dependencies, install NEONprocIS.base from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.stat/renv.lock")' && \
    R -e 'renv::install("/pack/NEONprocIS.stat")' && \
    rm -rf /pack


