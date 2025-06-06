# Dockerfile for NEON IS Data Processing - Regularization

# Start with the neon-is-base-r image. 
# Use a multi-stage build to obscure the value of GITHUB_PAT used to install eddy4R.base
#FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.5.2
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.5.2

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.rglr"

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org


# Install eddy4R.base from development branch (only until merged into open source version) 
# NOTE: Currently the eddy4R development repo is private and requires
# Sys.setenv(GITHUB_PAT = "MyAccessToken") to be assigned in R. Thus, we will pass this environment 
# variable in from the host, clone the repo in this intermediate build, then copy the relevant package 
# into the final build and install from there. To pass in the environment variable holding the token,
# use: docker build --build-arg auth_token=$GITHUB_PAT ...
#ARG auth_token
#ENV GITHUB_PAT=$auth_token
#
# Clone the NEON-FIU-algorithm repo using our token
#RUN git clone -b deve https://$GITHUB_PAT:x-oauth-basic@github.com/NEONScience/NEON-FIU-algorithm.git
#
# Now make the final build, which will leave behind our access token
#FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.5.2
#
# copy the eddy4R.base package from the previous image
#COPY --from=intermediate /NEON-FIU-algorithm/ext/eddy4R/pack/eddy4R.base/ /pack/eddy4R.base/

# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Install the eddy4R.base package, including dependencies (usually this is done through renv.lock file when pulling from public repo - comment this line)
#RUN git clone -b deve https://github.com/NEONScience/eddy4R.git 
#RUN R -e 'renv::install("/pack/eddy4R.base",repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' 
#RUN R -e 'renv::install("./eddy4R/pack/eddy4R.base",repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' 

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/flow.rglr.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.rglr.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
