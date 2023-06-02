# Dockerfile for NEON IS Data Processing - Publication package
# Build this image using: docker build --no-cache -t neon-is-pack-pub-r <path-to-parent-directory>


# Start with the neon-is-base-r image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.1.3

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# copy the NEONprocIS.pub package from local
COPY ./ /pack/NEONprocIS.pub/

# Install devtools & renv (package manager), restore from specific versions of dependencies, install NEONprocIS.pub from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.pub/renv.lock")' && \
    R -e 'renv::install("/pack/NEONprocIS.pub")' && \
    rm -rf /pack


