FROM centos:7 

ENV ORACLE_INSTANT_PKGNAME="oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm"
ENV ORACLE_INSTANT_URI="https://neon-ci-software.s3.data.neonscience.org/oracle-instant/$ORACLE_INSTANT_PKGNAME"

RUN yum update -y && \
    yum install -y epel-release && \
    yum install -y https://rhel7.iuscommunity.org/ius-release.rpm && \
    curl -O "${ORACLE_INSTANT_URI}" 2>&1 && \
    yum install -y gcc python36u python36u-pip python36u-devel python36u-setuptools "${ORACLE_INSTANT_PKGNAME}" && \
    ln -sf /usr/bin/python3.6 /usr/bin/python3 && \
    echo "/usr/lib/oracle/19.3/client64/lib" > /etc/ld.so.conf.d/oracle-instant.conf && \
    ldconfig && \
    pip3.6 install --no-cache-dir --upgrade pip setuptools && \
    yum remove gcc -y && \
    yum autoremove -y && \
    yum clean all && \
    rm -f "/tmp/${ORACLE_INSTANT_PKGNAME}" && \
    pip3.6 install cx_Oracle --upgrade && \
    pip3.6 install environs && \
    pip3.6 install structlog && \
    pip3.6 install geojson