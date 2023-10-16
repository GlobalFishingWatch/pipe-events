FROM python:3.7

# Configure the working directory
RUN mkdir -p /opt/project
WORKDIR /opt/project

# Download and install google cloud. See the dockerfile at
# https://hub.docker.com/r/google/cloud-sdk/~/dockerfile/
ENV CLOUD_SDK_VERSION="438.0.0"

## Install python and its dependencies
RUN apt-get -qqy update &&  \
    apt-get install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev && \
    apt-get install -qqy python3 python3-dev curl gcc apt-transport-https lsb-release openssh-client git && \
    apt-get install -y python3-pip && \
    pip install -U crcmod --break-system-packages

# Instal gcloud sdk
RUN \
  export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
  echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
  apt-get update && \
  apt-get install -y google-cloud-sdk=${CLOUD_SDK_VERSION}-0 && \
  gcloud config set core/disable_usage_reporting true && \
  gcloud config set component_manager/disable_update_check true && \
  gcloud config set metrics/environment github_docker_image

# Download and install the cloudssql proxy and the client libraries
RUN \
  wget -q https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O /usr/local/bin/cloud_sql_proxy && \
  chmod +x /usr/local/bin/cloud_sql_proxy && \
  apt-get -y install postgresql-client

# Setup a volume for configuration and auth data
VOLUME ["/root/.config"]

COPY requirements.txt requirements.txt
# Setup local application dependencies
RUN pip install -r requirements.txt
COPY . /opt/project

# install
RUN pip install -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["scripts/run"]

