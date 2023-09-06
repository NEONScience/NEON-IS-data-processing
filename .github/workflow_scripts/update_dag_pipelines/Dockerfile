# Dockerfile with Pachyderm Python library

FROM python:3.11

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./update_pipelines.py .

#RUN [ "python", "./update_pipelines.py" ]