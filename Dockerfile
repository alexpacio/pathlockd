# ---- builder ----
FROM rust:1-alpine AS builder

# grpcio (pulled in by tikv-client) builds the gRPC C-core via cmake; bindgen is
# not required (checked-in bindings) but cmake/clang/pkg-config/openssl are.
# OPENSSL_STATIC=1 ensures a fully static binary compatible with distroless/static.
RUN apk add --no-cache \
        protobuf-dev cmake clang pkgconfig openssl-dev musl-dev

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src

# Pass e.g. RUSTFLAGS="-C target-cpu=x86-64-v4" for microarch-tuned builds.
ARG RUSTFLAGS=""
ENV RUSTFLAGS=${RUSTFLAGS}
ENV OPENSSL_STATIC=1
RUN cargo build --release --locked

# ---- runtime ----
FROM gcr.io/distroless/static-debian13 AS runtime

COPY --from=builder /build/target/release/pathlockd /usr/local/bin/pathlockd

EXPOSE 50051
ENV PATHLOCKD_LISTEN=0.0.0.0:50051

# distroless/static ships a built-in nonroot user (uid 65532).
USER nonroot

# Liveness/readiness via the daemon's own Health RPC (also verifies TiKV
# reachability). Uses the binary itself, so no extra tooling in the image.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/pathlockd", "--health-check"]

ENTRYPOINT ["/usr/local/bin/pathlockd"]
