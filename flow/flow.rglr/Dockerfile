# Dockerfile for NEON IS Data Processing - Regularization

# Start with the neon-is-base-r image. 
# Use a multi-stage build to obscure the value of GITHUB_PAT used to install eddy4R.base
FROM quay.io/battelleecology/neon-is-base-r:v0.0.29 as intermediate

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Install eddy4R.base - NOTE: Currently the eddy4R repo is private and requires
# Sys.setenv(GITHUB_PAT = "MyAccessToken") to be assigned in R. Thus, we will pass this environment 
# variable in from the host, clone the repo in this intermediate build, then copy the relevant package 
# into the final build and install from there. To pass in the environment variable holding the token,
# use: docker build --build-arg auth_token=$GITHUB_PAT ...
ARG auth_token
ENV GITHUB_PAT=$auth_token

# Clone the NEON-FIU-algorithm repo using our token
RUN git clone -b deve https://$GITHUB_PAT:x-oauth-basic@github.com/NEONScience/NEON-FIU-algorithm.git

# Now make the final build, which will leave behind our access token
FROM quay.io/battelleecology/neon-is-base-r:v0.0.29

# copy the eddy4R.base package from the previous image
COPY --from=intermediate /NEON-FIU-algorithm/ext/eddy4R/pack/eddy4R.base/ /pack/eddy4R.base/

# Install the eddy4R.base package, including dependencies
RUN R -e 'devtools::install("/pack/eddy4R.base",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && rm -rf /pack

# Copy in regularization workflow
COPY ./flow.rglr.R /flow.rglr.R


