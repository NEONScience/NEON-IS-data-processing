####
#
# This dockerfile will build an image to run the cron_monthly_and_pub_control module.
# Example command (run from modules/ directory in Docker context):
# docker build -t neon-is-pub-cntl -f ./modules/cron_monthly_and_pub_control/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi8/ubi:8.3

ARG MODULE_DIR="./modules"
ARG APP_DIR="cron_monthly_and_pub_control"
ARG CONTAINER_APP_DIR="/usr/src/app"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/populate_pub_months.sh ${CONTAINER_APP_DIR}/${APP_DIR}/populate_pub_months.sh

RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm  && \
    yum -y install jq && \
    yum clean all  && \
    rm -rf /var/cache/yum && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser && \
    chown appuser:appuser -R ${CONTAINER_APP_DIR} && \
    chmod +x ${CONTAINER_APP_DIR}/${APP_DIR}/populate_pub_months.sh

USER appuser