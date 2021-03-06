# Dockerfile for NEON IS Data Processing - Base image

# Use a multi-stage build to obscure the value of GITHUB_PAT used to install private repos
FROM rocker/rstudio as intermediate

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Install NEONprocIS.base - NOTE: Currently the repo is private and requires
# Sys.setenv(GITHUB_PAT_BE = "MyAccessToken") to be assigned in R. 

# Thus, we will pass this environment variable in from the host, 
# clone the repo in this intermediate build, then copy the relevant package 
# into the final build and install from there. To pass in the environment variable holding the token,
# use: docker build --build-arg auth_token=$GITHUB_PAT ...
ARG auth_token_be
ENV GITHUB_PAT_BE=$auth_token_be

# Install R AVRO library (super developmental version at present)
RUN git clone -b master https://$GITHUB_PAT_BE@github.battelleecology.org/BattelleEcology/avro.git

# Now make the final build, which will leave behind our access token
# Start with stable R version - note that this must match the R version in any renv.lock file in order
# to correctly install Bioconductor packages
FROM rocker/r-ver:3.6.0

# Required to install devtools
RUN apt-get update \
&& apt-get dist-upgrade -y \
&& RUNDEPS="fftw3 \
        libudunits2-0 \
        udunits-bin \
        hdf5-helpers \
        libhdf5-cpp-100 \
        libnetcdf11 \
        libhdf5-100 \
        libsz2 \
        libmysql++3v5 \
        libmariadbclient18 \
        libpng-tools \
        libproj-dev \
	      libssl-dev \
	      # Library for git via ssh key
	      ssh \
        libxml2-dev \
        mysql-common" \
&& BUILDDEPS="fftw3-dev \
             libudunits2-dev \
             libjpeg-dev \
             libtiff5-dev \
             libnetcdf-dev \
             libpng-dev \
             libhdf5-dev \
             libmysql++-dev \
             libv8-dev \
             " \
&& apt-get install -y $BUILDDEPS $RUNDEPS && \
   apt-get autoremove -y && \
	 apt-get autoclean -y

# Install git
RUN apt-get -y install git-core

# copy the avro repo from the previous image
COPY --from=intermediate /avro/ /avro/

RUN apt-get -y install cmake && \
    apt-get -y install libjansson-dev && \
    apt-get -y install libsnappy-dev && \
    apt-get autoremove -y && \
	  apt-get autoclean -y && \
	  rm -rf /var/lib/apt/lists/*

RUN cd avro/lang/c && \
    mkdir build && \
    cd build && \
    cmake .. \
        -DCMAKE_INSTALL_PREFIX=$PREFIX \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo && \
    make && \
    make test && \
    make install

# Copy ravro.so to root directory
COPY ./ravro.so /ravro.so

# Copy the NEONprocIS.base package from local
COPY ./ pack/NEONprocIS.base/

# Install devtools & renv (package manager), restore from specific versions of dependencies, install NEONprocIS.base from local, and clean up afterwards. 
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
ENV LIBARROW_BINARY=true
ENV ARROW_USE_PKG_CONFIG=false
ENV LIBARROW_MINIMAL=false
RUN R -e 'utils::install.packages(c("devtools"))' && \
    R -e 'remotes::install_github("rstudio/renv@0.9.3")' && \
    R -e 'renv::consent(provided=TRUE); renv::restore("pack/NEONprocIS.base/")' && \
#    R -e 'remotes::install_github("apache/arrow/r@apache-arrow-0.17.1")' && \
    R -e 'renv::install("/pack/NEONprocIS.base")' && \
    rm -rf /pack

# Set environment variables for logging
ENV LOG_TO=stdout
ENV LOG_FORMAT=plain
ENV LOG_LEVEL=info 

