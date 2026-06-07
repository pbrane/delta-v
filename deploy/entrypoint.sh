#!/bin/sh
#
# Shared entrypoint for all Delta-V Spring Boot daemons.
# Launches the daemon using classpath-based execution.
#
# Environment variables:
#   MAIN_CLASS           — baked into image at build time (from MANIFEST.MF Start-Class)
#   JAVA_OPTS            — JVM options (set in compose.yml per daemon)
#   OPENNMS_INSTANCE_ID  — Kafka RPC/Twin topic prefix (default: DeltaV)
#
if [ -z "$MAIN_CLASS" ]; then
    echo "ERROR: MAIN_CLASS not set" >&2
    exit 1
fi

# Horizon's SystemInfoUtils.getInstanceId() reads the JVM system property
# org.opennms.instance.id (default "OpenNMS") in a static initializer, and
# KafkaTopicProvider derives RPC/Twin topic names (<instanceId>.<location>.rpc-request,
# <instanceId>.twin.*) from it. It must be passed as a -D flag — the
# SystemPropertyBridgePostProcessor (Spring EnvironmentPostProcessor) does not
# run early enough / reliably, so the OPENNMS_INSTANCE_ID env var alone has no
# effect on topic naming. Default DeltaV so every delta-v daemon agrees with the
# minion-gateway's DeltaV.* listener patterns.
exec java $JAVA_OPTS \
    -Dorg.opennms.instance.id="${OPENNMS_INSTANCE_ID:-DeltaV}" \
    -cp "/opt/libs/priority/*:/opt/libs/external/*:/opt/libs/internal/*:/opt/libs/daemon/*:/opt/app/*" \
    "$MAIN_CLASS"
