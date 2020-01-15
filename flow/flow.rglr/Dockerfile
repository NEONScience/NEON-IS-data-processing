# Dockerfile for NEON IS Data Processing - Regularization

# Start with the neon-is-base-r image. 
# Use a multi-stage build to obscure the value of GITHUB_PAT used to install eddy4R.base
FROM quay.io/battelleecology/neon-is-base-r:v0.0.31 as intermediate

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Install the eddy4R.base package
RUN R -e 'remotes::install_github("NEONScience/eddy4R@1.0.1",subdir="pack/eddy4R.base",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))'

# Copy in regularization workflow
COPY ./flow.rglr.R /flow.rglr.R


