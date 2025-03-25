ARG APP
ARG PROXY

# Using the `rust-musl-builder` as base image, instead of 
# the official Rust toolchain
FROM clux/muslrust:stable AS chef
USER root

RUN mkdir -p /root/.cargo && \
    tee /root/.cargo/config.toml <<EOF
[source.crates-io]
replace-with = "ustc"

[source.ustc]
registry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"

[registries.ustc]
index = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"
EOF

RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
# Notice that we are specifying the --target flag!
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl --bin $APP

FROM alpine:3.20.1 AS runtime
ENV APP=$APP
ENV PROXY=$PROXY
RUN addgroup -S nqrs && adduser -S nqrs -G nqrs
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/$APP /usr/local/bin/
USER nqrs
CMD ["sh", "-c", "ALL_PROXY=$PROXY /usr/local/bin/$APP"]
