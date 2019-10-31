# Dockerfile for NEON IS Data Processing - Calibration package

# Start with the neon-is-base-r image. 
FROM quay.io/battelleecology/neon-is-base-r:v0.0.27

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# copy the NEONprocIS.qaqc package from local
COPY ./ /pack/NEONprocIS.cal/

# Install the NEONprocIS.qaqc package, including dependencies
RUN R -e 'devtools::install("/pack/NEONprocIS.cal",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && rm -rf /pack


