# syntax=docker/dockerfile:1
#
# Hermetic build: dependencies are vendored, so no module download (or private
# registry credentials) is needed at image-build time.

FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOFLAGS=-mod=vendor \
    go build -ldflags="-s -w" -o /out/medusa-node ./cmd/medusa-node

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/medusa-node /medusa-node
# 7700 = data plane (Poseidon h2c); 8080 = admin/health HTTP.
EXPOSE 7700 8080
USER nonroot:nonroot
ENTRYPOINT ["/medusa-node"]
