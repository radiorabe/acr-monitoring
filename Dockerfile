FROM ghcr.io/radiorabe/s2i-python:2.1.4 AS build

COPY --chown=1001:0 ./ /opt/app-root/src/

RUN    python -mbuild


FROM ghcr.io/radiorabe/python-minimal:2.1.2 AS app

COPY --from=build /opt/app-root/src/dist/*.whl /tmp/dist/

RUN    microdnf install -y \
         python3.11-pip \
    && python -mpip --no-cache-dir install /tmp/dist/*.whl \
    && microdnf remove -y \
         python3.11-pip \
         python3.11-setuptools \
    && microdnf clean all \
    && rm -rf /tmp/dist/

COPY LICENSE /license/

USER 1001

CMD ["acrmonitor"]
