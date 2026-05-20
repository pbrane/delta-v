# Copyright (C) 2026 BeaconStrategists, Inc.
# Licensed under the GNU Affero General Public License v3.
FROM eclipse-temurin:21-jre-alpine

RUN apk add --no-cache curl && \
    addgroup -g 10001 deltav && \
    adduser -u 10001 -G deltav -D deltav

COPY --chown=deltav:deltav target/alerts-forwarder.jar /app/alerts-forwarder.jar

USER deltav
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/alerts-forwarder.jar"]
