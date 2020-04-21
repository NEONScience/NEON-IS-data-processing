FROM centos:7

ARG APP_DIR="location_asset"
ARG LIB_DIR="lib"
ARG DATA_ACCESS_DIR="data_access"
ARG ORACLE_INSTANT_PKGNAME="oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm"
ARG ORACLE_INSTANT_URI="https://neon-ci-software.s3.data.neonscience.org/oracle-instant/$ORACLE_INSTANT_PKGNAME"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN yum update -y && \
    yum install -y epel-release && \
    curl -O "${ORACLE_INSTANT_URI}" 2>&1 && \
    yum install -y gcc python36 python36-pip python36-devel python36-setuptools "${ORACLE_INSTANT_PKGNAME}" && \
    echo "/usr/lib/oracle/19.3/client64/lib" > /etc/ld.so.conf.d/oracle-instant.conf && \
    ldconfig && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools && \
    python3 -mpip install -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    yum remove gcc -y && \
    yum autoremove -y && \
    yum clean all && \
    rm -f "/tmp/${ORACLE_INSTANT_PKGNAME}" && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${LIB_DIR} ${CONTAINER_APP_DIR}/${LIB_DIR}
COPY ${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}

USER appuser