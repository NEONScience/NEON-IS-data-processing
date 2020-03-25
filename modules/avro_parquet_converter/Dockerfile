FROM quay.io/battelleecology/python_parent:3

RUN yum install -y gcc && \
    pip3.6 install --upgrade pip && \
    pip3.6 install pyarrow && \
    pip3.6 install pandas && \
    pip3.6 install spavro && \
    yum remove gcc -y && \
    yum autoremove -y && \
    yum clean all

ENV APP_DIR="avro_parquet_converter"
ENV LIB_DIR="lib"

ENV CONTAINER_ROOT="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:/usr/src/app"

WORKDIR ${CONTAINER_ROOT}

ADD ${APP_DIR} ${CONTAINER_ROOT}/${APP_DIR}
ADD ${LIB_DIR} ${CONTAINER_ROOT}/${LIB_DIR}