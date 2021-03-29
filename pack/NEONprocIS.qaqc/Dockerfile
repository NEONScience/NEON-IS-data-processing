# Dockerfile for NEON IS Data Processing - QAQC package

# Start with the neon-is-base-r image. # Use a multi-stage build to obscure the value of GITHUB_PAT used to install eddy4R.base & eddy4R.qaqc
FROM quay.io/battelleecology/neon-is-base-r:v0.0.64 as intermediate

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org




# Install eddy4R.qaqc from development branch (only until merged into open source version) 
# NOTE: Currently the eddy4R development repo is private and requires
# Sys.setenv(GITHUB_PAT = "MyAccessToken") to be assigned in R. Thus, we will pass this environment 
# variable in from the host, clone the repo in this intermediate build, then copy the relevant package 
# into the final build and install from there. To pass in the environment variable holding the token,
# use: docker build --build-arg auth_token=$GITHUB_PAT ...
ARG auth_token
ENV GITHUB_PAT=$auth_token

# Clone the NEON-FIU-algorithm repo using our token
RUN git clone -b deve https://$GITHUB_PAT:x-oauth-basic@github.com/NEONScience/NEON-FIU-algorithm.git

# Now make the final build, which will leave behind our access token
FROM quay.io/battelleecology/neon-is-base-r:v0.0.64

# copy the eddy4R.base package from the previous image
COPY --from=intermediate /NEON-FIU-algorithm/ext/eddy4R/pack/eddy4R.base/ /pack/eddy4R.base/
COPY --from=intermediate /NEON-FIU-algorithm/ext/eddy4R/pack/eddy4R.qaqc/ /pack/eddy4R.qaqc/


# copy the NEONprocIS.qaqc package from local
COPY ./ /pack/NEONprocIS.qaqc/






# Install the eddy4R.base, eddy4R.qaqc, and NEONprocIS.qaqc packages, including dependencies (NOT RUN, but the top two commands are how one would install the open source/production version of eddy4R on local prior to creating the lock file so that renv::restore puts them in the image)
#RUN R -e 'remotes::install_github("NEONScience/eddy4R@1.0.1",subdir="pack/eddy4R.base",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && \
#    R -e 'remotes::install_github("NEONScience/eddy4R@1.0.1",subdir="pack/eddy4R.qaqc",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && \
#    R -e 'devtools::install("/pack/NEONprocIS.qaqc",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && rm -rf /pack

# Install development versions of eddy4R.base and eddy4R.qaqc
#RUN R -e 'devtools::install("/pack/eddy4R.base",dependencies=TRUE,repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))'
#RUN R -e 'Sys.setenv(R_REMOTES_NO_ERRORS_FROM_WARNINGS=TRUE); devtools::install("/pack/eddy4R.qaqc",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))'

# Restore from specific versions of dependencies, install NEONprocIS.qaqc from local, and clean up afterwards. 
RUN R -e 'renv::restore(lockfile="pack/NEONprocIS.qaqc/renv.lock")' && \
    R -e 'devtools::install("/pack/eddy4R.base",dependencies=FALSE,repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && \
    R -e 'devtools::install("/pack/eddy4R.qaqc",dependencies=FALSE,repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && \
    R -e 'renv::install("/pack/NEONprocIS.qaqc")' && rm -rf /pack && \
    rm -rf /pack
    
  