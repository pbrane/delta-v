# Envoy ingress for the Minion-facing gRPC surface.
#
# rc1 posture: h2c plaintext on :8443 inside the compose network. TLS
# termination on this listener arrives in rc2 (per Phase 0 Decision 4).
# Long-lived bidi streams: stream/route timeouts disabled (Decision 3).

FROM envoyproxy/envoy:v1.30.4

# curl is needed by the docker-compose healthcheck (the upstream Envoy
# image strips all HTTP clients). Install minimally; cleanup apt lists
# to keep the image small. The upstream image runs as root by default
# (intentional, to allow binding privileged ports); preserve that.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY envoy.yaml /etc/envoy/envoy.yaml

EXPOSE 8443 9901

CMD ["/usr/local/bin/envoy", "-c", "/etc/envoy/envoy.yaml", "--service-cluster", "minion-edge"]
