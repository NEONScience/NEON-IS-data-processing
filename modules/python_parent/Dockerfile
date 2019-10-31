FROM centos:7

RUN yum update -y && \
    yum install -y epel-release && \
    yum install -y https://rhel7.iuscommunity.org/ius-release.rpm && \
    yum install -y gcc python36u python36u-pip python36u-devel python36u-setuptools && \
    ln -sf /usr/bin/python3.6 /usr/bin/python3 && \
    ldconfig && \
    pip3.6 install --no-cache-dir --upgrade pip setuptools && \
    yum remove gcc -y && \
    yum autoremove -y && \
    yum clean all && \
    pip3.6 install structlog && \
    pip3.6 install environs