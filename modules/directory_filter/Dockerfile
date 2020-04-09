FROM centos:7

ARG APP_DIR="directory_filter"
ARG LIB_DIR="lib"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${LIB_DIR} ${CONTAINER_APP_DIR}/${LIB_DIR}

RUN yum update -y && \
    yum install -y epel-release && \
    yum install -y https://rhel7.iuscommunity.org/ius-release.rpm && \
    yum install -y gcc python36u python36u-pip python36u-devel python36u-setuptools && \
    ln -sf /usr/bin/python3.6 /usr/bin/python3 && \
    ldconfig && \
    pip3.6 install --no-cache-dir --upgrade pip setuptools && \
    pip install -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    yum remove gcc -y && \
    yum autoremove -y && \
    yum clean all && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

USER appuser