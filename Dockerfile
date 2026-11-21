# Shopping Sessions: one static Rust binary on an empty image.
FROM rust:1-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY .cargo .cargo
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/pipe /pipe
USER 1000:1000
ENTRYPOINT ["/pipe"]
