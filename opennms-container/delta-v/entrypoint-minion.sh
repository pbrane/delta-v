#!/bin/sh
set -e

JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx512m}"
SPRING_ARGS=""

# Legacy env var bridge (operator contract)
[ -n "$MINION_ID" ]        && SPRING_ARGS="$SPRING_ARGS --opennms.minion.id=$MINION_ID"
[ -n "$MINION_LOCATION" ]  && SPRING_ARGS="$SPRING_ARGS --opennms.minion.location=$MINION_LOCATION"
[ -n "$MINION_LOG_LEVEL" ] && SPRING_ARGS="$SPRING_ARGS --logging.level.org.opennms=$MINION_LOG_LEVEL"

# Kafka IPC bridge
[ -n "$KAFKA_IPC_BOOTSTRAP_SERVERS" ] && SPRING_ARGS="$SPRING_ARGS --opennms.kafka.bootstrap-servers=$KAFKA_IPC_BOOTSTRAP_SERVERS"

# Optional JMX
if [ -n "$MINION_JMX_PORT" ]; then
    JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=$MINION_JMX_PORT"
    JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.rmi.port=$MINION_JMX_PORT"
    JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
    JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"
fi

# Optional: load operator's minion-config.yaml
if [ -f /etc/opennms/minion/minion-config.yaml ]; then
    SPRING_ARGS="$SPRING_ARGS --spring.config.additional-location=file:/etc/opennms/minion/minion-config.yaml"
fi

exec java $JAVA_OPTS -jar /opt/daemon-boot-minion.jar $SPRING_ARGS
