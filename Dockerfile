FROM python:3.12-slim AS prod

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Assets (SQL templates, schemas) are loaded via relative paths (./assets/...),
# so the runtime working directory must be the project root.
WORKDIR /opt/project

# Setup local application dependencies
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /opt/project
RUN pip install . --no-deps --no-cache-dir && \
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
