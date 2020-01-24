# Dockerfile for NEON IS Data Processing - Regularization

# Start with the neon-is-base-r image. 
# Use a multi-stage build to obscure the value of GITHUB_PAT used to install eddy4R.base
FROM quay.io/battelleecology/neon-is-base-r:v0.0.35

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in regularization workflow
COPY ./flow.rglr.R /flow.rglr.R


