# Dockerfile for NEON IS Data Processing - QAQC package

# Start with the neon-is-base-r image. # Use a multi-stage build to obscure the value of GITHUB_PAT used to install eddy4R.base & eddy4R.qaqc
FROM quay.io/battelleecology/neon-is-base-r:v0.0.49 

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# copy the NEONprocIS.qaqc package from local
COPY ./ /pack/NEONprocIS.qaqc/

# Install the eddy4R.base, eddy4R.qaqc, and NEONprocIS.qaqc packages, including dependencies (NOT RUN, but the top two commands are how one would install eddy4R on local prior to creating
# the lock file so that renv::restore puts them in the image)
#RUN R -e 'remotes::install_github("NEONScience/eddy4R@1.0.1",subdir="pack/eddy4R.base",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && \
#    R -e 'remotes::install_github("NEONScience/eddy4R@1.0.1",subdir="pack/eddy4R.qaqc",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && \
#    R -e 'devtools::install("/pack/NEONprocIS.qaqc",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && rm -rf /pack


# Restore from specific versions of dependencies, install NEONprocIS.qaqc from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.qaqc/renv.lock")' && \
    R -e 'renv::install("/pack/NEONprocIS.qaqc")' && \
    rm -rf /pack