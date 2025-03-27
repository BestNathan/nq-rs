FROM alpine:latest AS fluvio

ARG FLUVIO_HOST=127.0.0.1

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories
RUN apk add --no-cache --update curl

# install fluvio
RUN curl -fsS https://hub.infinyon.cloud/install/install.sh | sh
ENV PATH="/root/.fluvio/bin:$PATH"
RUN export PATH

WORKDIR /app

# Download specific connector (change to your connector)
RUN cdk hub download -o http-source.ipkg infinyon/http-source@0.4.3
RUN tar -xf http-source.ipkg
RUN tar -xzf manifest.tar.gz

# Export fluvio profile
RUN fluvio profile add fluvio $FLUVIO_HOST:9103 docker
RUN fluvio profile export > fluvio_profile.toml

# setup runtime container
FROM alpine:latest

ENV RUST_LOG=info

# setup fluvio as non user
RUN adduser -h /home/fluvio -D fluvio
USER fluvio
WORKDIR /home/fluvio/connector

# Copy connector configuration
COPY --from=fluvio /app/http-source /home/fluvio/connector/http-source
COPY --from=fluvio /app/fluvio_profile.toml /home/fluvio/.fluvio/config

# run http-source, this will be different for each connector
CMD ["/home/fluvio/connector/http-source",  "--config", "/home/fluvio/connector/connector.yaml"]