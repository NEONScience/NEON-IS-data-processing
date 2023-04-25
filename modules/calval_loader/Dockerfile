####
# This dockerfile will build the caliration repo loader module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t group_loader:latest -f caliration_repo_loader/Dockerfile .
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG APP_DIR="calval_loader"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"
ENV CVAL_INGEST_BUCKET="neon-cval"
ENV GCP_PACHY_WRITER_JSONKEY='{\
  "type": "service_account",\
  "project_id": "neon-nonprod-longtermstorage",\
  "private_key_id": "df389ebb177538dfa50fccd336282d4214848f7f",\
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDlmJBNIhsZm/Yr\nU1FnbuxhzC+EjlnkQXR8lMEsfwb5hk7OGNH6OadcEBOb1ziUWiTh+2Cva0cEaq3R\no4+BNo9nFkQuLmTb8Rm0S2a/7Y4tY9/nnmyx0fJLe900stM817u0im/1qJ7Gli9P\nP3FJV6qcyZSJUMNlVHcrpSu8yADqcwC0BU7PsCYD+wRaRfb4NxXSoSgXMFXV+ScT\nJNRAPdkh7+WIfwT5+zl0WXZWPhRPN6zqE3ONjsj5IC1+90Ozjwt9rTnqJ7oFYLGZ\nTjQa7/8YJb8/mga5Ud7xNdSDsLNW9F8Qm3RFvninbLY9K1pdlC+1T8iK40f6AwfX\nb38ODA/3AgMBAAECggEALFGLRUBYn9Rhq9JM69zXBze9UmaSBoP2AthKA5dPltns\nm07iNV1sp1I5HsNoGgbeiLqX2rSoBDu9arKaUFRiUQFUzt9Iu8Xp8sn9Pv4RjERz\no4eKrrZ6gUSbGgGM0flIs9xnKTHP2ECESFVUPlaMSfCFWg47cXNg/41GCI5I0vhb\naLAuBe3PUYWF7AznInnw1NGk7ozCbshTz03tfiv7zVsK3XGMNwMRFc5D4x6ekFUh\nQKJf/RBbRbVtIYDsHizxtUboEho5Px4Z4qM5W+dFaeeLNKBLNypIrFvrnBdIAdWp\nUV2V5ROPM5NuWvdEnkyjS5JZmjucdx/bKAoX/R+gDQKBgQDz/UeDfe0l35qxS1W2\n2WzUsHSl1yVQdJ09zmikxPKML9EWm819F9awZ4/xXQKfOYQDC9f9bKPc0CiQwZC/\nV0SfMp4+amX2tydxk1gC/C9OVpWeJddG6PaVfA+RF/nhLJErKWfLcHL8RPSzz59B\n/t+mzFGs4npgfcFkvpCqiVhYIwKBgQDw5eaBZ2XTzsJr20S7xubsUVM3Qqt+435m\n9sUNmAUm2qY5u3r9mNdH9lkc2Ygo2GRQqTCxdnroUFVCXZ7nNo8L/njCy+5Bbo3m\nEV9uXOVaMQkruzybDi28+VTZG90bLQ4oCLHmE0JvQjWYXHSEpKg4FPaYyaTHk/eG\noHBWzkTcHQKBgCVYPAztRlIIspW2cS0mhFjdlU0Y7BYvcy3VnQCzQGiinP8347Dk\n6DBh+Y847EDQvdr1gh7A9SfEGKFnby9KPS+cwMnO4UJs8DWbFtqGHPuBly74ncW1\nvKBOt9c+czm655FSqhdiY8cnCslG2xpv122hUlMC6zY/qU0xOdVQZlw/AoGAYgOY\ncuRICRLiBhJT9/botBQuwJguwNT89BXJfUtyJSnnYiwG0bHjPdNc1KEphjztK+h6\n+GmVfnpZBkSjR73qG94w8kkZKqQ5tfcxlezUIIs9CP8InKn7MB1eSWvN3aXKUb2f\n7FW/vRb6SYCso99+1jqpTucUVWO93ZffvWH1oH0CgYAilGEExcEySbASIDCTQlTn\n+WjPq0CyS7HuIe7r+OSKVh18WR9s22KD/fBA9XBQ5gbEI188K3NOzzYy+3Yq4T2p\nNqJTd7XHCyWybeqmz1VgOuM/RFO8KSop3pJpdnuMf5j0Ghp/rsb/WkfweNaaZc3a\ni4NYUBhTIvniYIPpvBM0HQ==\n-----END PRIVATE KEY-----\n",\
  "client_email": "test-pachy-writer@neon-nonprod-longtermstorage.iam.gserviceaccount.com",\
  "client_id": "110400800036531486198",\
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",\
  "token_uri": "https://oauth2.googleapis.com/token",\
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",\
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-pachy-writer%40neon-nonprod-longtermstorage.iam.gserviceaccount.com"\
}'

WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/app-requirements.txt


RUN update-ca-trust && \
    microdnf update -y --disableplugin=subscription-manager && \
    microdnf install -y --disableplugin=subscription-manager \
            shadow-utils \
            gcc \
            libzstd  \
            python39 \
            python39-pip \
            python39-wheel \
            python39-devel \
            python39-setuptools && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/app-requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}

USER appuser