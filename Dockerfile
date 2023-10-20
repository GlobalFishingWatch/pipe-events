FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-bash-pipeline:latest

# Configure the working directory
RUN mkdir -p /opt/project
WORKDIR /opt/project

# Setup local application dependencies
COPY . /opt/project

RUN apt-get -qqy update && apt-get -qqy upgrade

# Install
RUN pip install -r requirements.txt
RUN pip install -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["scripts/run"]

