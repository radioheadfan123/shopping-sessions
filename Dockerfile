# Shopping Sessions: one Rust binary. Build and run images share Debian trixie, so glibc matches.
FROM rust:1-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY .cargo .cargo
RUN cargo build --release --locked

FROM debian:trixie-slim
COPY --from=build /src/target/release/pipe /pipe
ENV POP=1
USER 1000:1000
ENTRYPOINT ["/pipe"]
