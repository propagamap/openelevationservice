FROM ubuntu:mantic AS builder

RUN apt-get update
RUN apt-get -y install build-essential python3-dev python3-venv

RUN mkdir -p /deploy/app

COPY openelevationservice /deploy/app/openelevationservice
COPY ops_settings_docker.yml /deploy/app/openelevationservice/server/ops_settings.yml
COPY run_grpc_server.py /deploy/app/run_grpc_server.py
COPY requirements.txt /deploy/app/requirements.txt

RUN python3 -m venv /oes_venv
RUN /bin/bash -c "source /oes_venv/bin/activate"
RUN /oes_venv/bin/pip3 install wheel
RUN /oes_venv/bin/pip3 install -r /deploy/app/requirements.txt

FROM ubuntu:mantic

LABEL org.opencontainers.image.source=https://github.com/propagamap/openelevationservice

COPY --from=builder /deploy /deploy
COPY --from=builder /oes_venv /oes_venv

RUN apt-get update \
    && /bin/bash -c "DEBIAN_FRONTEND=noninteractive apt-get -y --no-install-recommends install locales postgis postgresql-client python3-venv tzdata" \
    && ln -fs /usr/share/zoneinfo/Europe/Berlin /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && locale-gen en_US.UTF-8 \
    && apt-get -y --purge autoremove \
    && apt-get clean \
    && /bin/bash -c "source /oes_venv/bin/activate"

# Set the locale
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
# oes/flask variables
ENV OES_LOGLEVEL INFO

WORKDIR /deploy/app

EXPOSE 5005

# Start server
CMD ["/oes_venv/bin/python3", "run_grpc_server.py"]
