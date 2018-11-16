FROM python:2.7

# Configure the working directory
RUN mkdir -p /opt/project
WORKDIR /opt/project

# Install and update pip
# Pin the version because pip>=10.0 does not support the --download flag  which is required for dataflow
RUN pip install -U --ignore-installed pip==9.0.3

# Download and install google cloud. See the dockerfile at
# https://hub.docker.com/r/google/cloud-sdk/~/dockerfile/
RUN  \
  export CLOUD_SDK_APT_DEPS="curl gcc python-dev python-setuptools apt-transport-https lsb-release openssh-client git" && \
  export CLOUD_SDK_PIP_DEPS="crcmod" && \
  apt-get -y update && \
  apt-get install -y $CLOUD_SDK_APT_DEPS && \
  pip install -U $CLOUD_SDK_PIP_DEPS && \
  export CLOUD_SDK_VERSION="198.0.0" && \
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

# Download and install numpy and gdal
RUN apt-get -y update && \
  apt-get -y install gdal-bin libgdal-dev python-numpy-dev && \
  pip install numpy

# Setup a volume for configuration and auth data
VOLUME ["/root/.config"]

# Setup local application dependencies
COPY . /opt/project

# install
RUN pip install --process-dependency-links -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["scripts/run"]

