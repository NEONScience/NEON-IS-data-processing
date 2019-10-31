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
# Start with stable R version
FROM rocker/r-ver:3.5.2

# copy the NEONprocIS.base package from local
COPY ./ pack/NEONprocIS.base/


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
             " \
&& apt-get install -y $BUILDDEPS $RUNDEPS

# Install git
RUN apt-get -y install git-core

# copy the avro repo from the previous image
COPY --from=intermediate /avro/ /avro/

RUN apt-get -y install cmake && \
    apt-get -y install libjansson-dev && \
    apt-get -y install libsnappy-dev

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

# Clean up files
RUN apt-get autoremove -y && \
	  apt-get autoclean -y && \
	  rm -rf /var/lib/apt/lists/*
	  
# Install devtools package
RUN R -e 'utils::install.packages("devtools")'

# Install the NEONprocIS package, including dependencies, and clean up afterwards
RUN R -e 'devtools::install("/pack/NEONprocIS.base",dependencies=c(NA, TRUE)[2],repos=c(remotes::bioc_install_repos(),"https://cran.rstudio.com/"))' && \
    rm -rf /pack

# Set environment variables for logging
ENV LOG_TO=stdout
ENV LOG_FORMAT=plain
ENV LOG_LEVEL=info 


