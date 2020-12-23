# Dockerfile for NEON IS Data Processing - Regularization

# Start with the neon-is-base-r image. 
# Use a multi-stage build to obscure the value of GITHUB_PAT used to install eddy4R.base
#FROM quay.io/battelleecology/neon-is-base-r:v0.0.60
FROM quay.io/battelleecology/neon-is-base-r:v0.0.60 as intermediate

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org




# Install eddy4R.base from development branch (only until merged into open source version) 
# NOTE: Currently the eddy4R development repo is private and requires
# Sys.setenv(GITHUB_PAT = "MyAccessToken") to be assigned in R. Thus, we will pass this environment 
# variable in from the host, clone the repo in this intermediate build, then copy the relevant package 
# into the final build and install from there. To pass in the environment variable holding the token,
# use: docker build --build-arg auth_token=$GITHUB_PAT ...
ARG auth_token
ENV GITHUB_PAT=$auth_token

# Clone the NEON-FIU-algorithm repo using our token
RUN git clone -b master https://$GITHUB_PAT:x-oauth-basic@github.com/covesturtevant/NEON-FIU-algorithm-covesturtevant.git

# Now make the final build, which will leave behind our access token
FROM quay.io/battelleecology/neon-is-base-r:v0.0.60

# copy the eddy4R.base package from the previous image
COPY --from=intermediate /NEON-FIU-algorithm-covesturtevant/ext/eddy4R/pack/eddy4R.base/ /pack/eddy4R.base/






# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Install the eddy4R.base package, including dependencies (usually this is done through renv.lock file when pulling from public repo - comment this line)
RUN R -e 'devtools::install("/pack/eddy4R.base",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))'

# Copy in regularization workflow
COPY ./flow.rglr.R /flow.rglr.R
