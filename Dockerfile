FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-bash-pipeline:latest-python3.8 AS prod

# Setup local application dependencies
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . /opt/project
RUN pip install . --no-deps && \
    rm -rf /root/.cache/pip && \
    rm -rf /opt/project/*

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["pipe-events"]

# TEMPORARY (until these are properly installed in the package)
COPY ./assets /opt/project/assets

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE (editable install and development tools)
# ---------------------------------------------------------------------------------------
FROM prod AS dev

COPY . /opt/project
RUN pip install -e .[dev]
RUN pip install -r requirements-test.txt

# ---------------------------------------------------------------------------------------
# TEST IMAGE (This checks that package is properly installed in prod image)
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./tests /opt/project/tests
COPY ./requirements-test.txt /opt/project/

RUN pip install -r requirements-test.txt
