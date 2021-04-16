# Dockerfile for NEON IS Data Processing - Water Quality package

# Start with the calibration package image. 
FROM quay.io/battelleecology/neon-is-pack-cal-r:v0.0.40

# maintainer handle
MAINTAINER "Kaelin Cawley" kcawley@battelleecology.org

# copy the NEONprocIS.qaqc package from local
COPY ./ /pack/NEONprocIS.wq/

# Install devtools & renv (package manager), restore from specific versions of dependencies, install NEONprocIS.base from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.wq/renv.lock")' && \
    R -e 'renv::install("/pack/NEONprocIS.wq")' && \
    rm -rf /pack

