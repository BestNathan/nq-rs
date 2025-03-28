FROM alpine:latest AS fluvio

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories
RUN apk add --no-cache --update curl

# install fluvio
RUN curl -fsS https://hub.infinyon.cloud/install/install.sh | sh
ENV PATH="/root/.fluvio/bin:$PATH"
RUN export PATH

WORKDIR /app

ARG FLUVIO_HOST=127.0.0.1
ARG FLUVIO_PORT=9103
ARG FLUVIO_INSTALLATION_TYPE=docker

# Export fluvio profile
RUN fluvio profile add fluvio $FLUVIO_HOST:$FLUVIO_PORT $FLUVIO_INSTALLATION_TYPE
RUN fluvio profile export > fluvio_profile.toml

FROM fluvio AS fluvio-connector

WORKDIR /app

ARG FLUVIO_HTTP_SINK_CONNECTOR_VERSION=0.2.12

# Download specific connector
RUN cdk hub download -o http-sink.ipkg infinyon/http-sink@$FLUVIO_HTTP_SINK_CONNECTOR_VERSION
RUN tar -xf http-sink.ipkg
RUN tar -xzf manifest.tar.gz

# setup runtime container
FROM alpine:latest AS runtime

ENV RUST_LOG=info

# setup fluvio as non user
RUN adduser -h /home/fluvio -D fluvio
USER fluvio
WORKDIR /home/fluvio/connector

# Copy connector configuration
COPY --from=fluvio-connector /app/http-sink /home/fluvio/connector/http-sink
COPY --from=fluvio /app/fluvio_profile.toml /home/fluvio/.fluvio/config

CMD ["./http-sink",  "--config", "./connector.yaml", "--secrets", "./secrets"]