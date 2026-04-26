# minion-gateway — gRPC ingress translator for Minion-facing surface.
#
# Hosts the org.deltav.minion.grpc.v1 service surface (Heartbeat in rc1;
# Sink/RPC channels migrate in v1.3) and translates each gRPC call into
# the corresponding Kafka topic publish. Plaintext h2c in rc1; TLS is
# terminated upstream by Envoy in v1.2.0+ and on the listener directly
# in rc2.
#
# Build (ad-hoc until wired into build.sh):
#   ./mvnw -pl core/minion-gateway -am clean install -DskipTests
#   docker build \
#     -t deltav/minion-gateway:${VERSION} \
#     -t deltav/minion-gateway:latest \
#     -f opennms-container/delta-v/minion-gateway/Dockerfile \
#     core/minion-gateway/

ARG JRE_BASE=deltav/jre-deltav:21
FROM ${JRE_BASE}

LABEL org.opennms.image.role="minion-gateway"
LABEL org.opennms.image.responsibility="gRPC to Kafka translator for Minion-facing surface"

ENV APP_HOME=/opt/deltav/minion-gateway
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

WORKDIR ${APP_HOME}

COPY target/org.opennms.core.minion-gateway-*.jar ${APP_HOME}/minion-gateway.jar

EXPOSE 50051 8080

ENTRYPOINT ["java", "-jar", "/opt/deltav/minion-gateway/minion-gateway.jar"]
