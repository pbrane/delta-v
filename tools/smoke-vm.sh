#!/bin/bash
#
# smoke-vm.sh — Delta-V post-publish smoke test for a clean VM.
#
# Validates the no-git-clone contract: curl the published compose files for a
# release tag and run the stack from images alone. There is intentionally NO
# source tree / Makefile on the smoke VM — this is the raw `docker compose`
# path a real image-only consumer uses (the `make` front door is for developers
# with a checkout). See the "Delta-V smoke test procedure" reference for the
# wider rationale.
#
# Usage on a clean VM (post tag-push + image publish):
#   1. Copy this script to the VM (or curl it from the tag).
#   2. Set GIT_REF / IMG_TAG below to the cut you're smoking.
#   3. ./smoke-vm.sh
#
# Hard reset first (intentional — leftover state from a prior cut pollutes the
# next attempt). This removes ALL containers/volumes/images on the host.

cd ~ && docker rm -f $(docker ps -aq) 2>/dev/null
docker volume rm -f $(docker volume ls -q) 2>/dev/null
docker system prune -a --volumes -f
rm -rf ~/delta-v-smoke
mkdir -p ~/delta-v-smoke && cd ~/delta-v-smoke

GIT_REF=v1.3.0-rc10
IMG_TAG=1.3.0-rc10

# dns-lab: also start the CoreDNS 'dns-lab' profile and point the flow-enricher at
# it (DELTAV_FLOWS_DNS_NAMESERVERS=172.18.0.53). It synthesizes a PTR for any 10/8
# query, so nl6's synthetic 10/8 flow IPs reverse-resolve to nl6-host-*.lab and
# show as hostnames (not bare IPs) in the flows dashboards. Set to false for a
# pure release-validation run (the published compose already carries the wiring;
# this just toggles the profile + nameserver).
DNS_LAB=true

BASE=https://raw.githubusercontent.com/pbrane/delta-v/$GIT_REF/deploy

curl -OL $BASE/compose.yml
curl -OL $BASE/compose.override.dev.yml

# Profile set + the one extra .env line the dns-lab path needs.
PROFILES="--profile demo"
DNS_NS_LINE=""
if [ "$DNS_LAB" = "true" ]; then
    PROFILES="$PROFILES --profile dns-lab"
    DNS_NS_LINE="DELTAV_FLOWS_DNS_NAMESERVERS=172.18.0.53"
fi

cat > .env <<EOF
IMAGE_PREFIX=ghcr.io/pbrane
VERSION=$IMG_TAG
$DNS_NS_LINE
EOF

# 'demo' profile (rc3+) = full daemon stack + the whole observability pipeline
# (victoriametrics, vmagent, prometheus-writer, grafana, alertmanager,
#  alerts-forwarder) + the Track 3 alarms-materializer — one token replaces the
# old '--profile full --profile metrics'. compose.override.dev.yml keeps the lean
# JVM sizing for the resource-constrained VM (raw compose doesn't auto-layer it
# the way 'make up PROFILE=demo' does). $PROFILES also adds dns-lab when DNS_LAB=true.
docker compose -f compose.yml -f compose.override.dev.yml $PROFILES pull
docker compose -f compose.yml -f compose.override.dev.yml $PROFILES up -d

# --- Print all browser-accessible URLs ---
HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
HOST_IP="${HOST_IP:-localhost}"

cat <<EOF

==============================================================
 Delta-V $IMG_TAG is starting on $HOST_IP
==============================================================

 Observability
   Grafana                http://$HOST_IP:13000           (admin/admin)
   VictoriaMetrics UI     http://$HOST_IP:18428
   Alertmanager           http://$HOST_IP:9093
   Prometheus Writer      http://$HOST_IP:18080/actuator/health

 Data plane
   ClickHouse HTTP        http://$HOST_IP:8123/play
   nl6 REST/UI            http://$HOST_IP:19081

 Daemon actuators
   Minion                 http://$HOST_IP:8301/actuator/health
   Bsmd                   http://$HOST_IP:8180/actuator/health

 Ingress (not browser URLs, FYI)
   Minion gateway (gRPC)  $HOST_IP:8443
   Kafka bootstrap        $HOST_IP:19092
   SNMP test agent        $HOST_IP:19161/udp
   Trapd / Syslog / Flow  $HOST_IP:11162/udp, 1514/udp, 4729/udp

 Track 3 (alarms-materializer): no host port — check health with
   'docker compose ps alarms-materializer'; its metrics appear in Grafana/VM
   (deltav_alarms_materializer_*). Default persistence.mode is 'dual-write'.

 Tip: 'docker compose ps' to watch health; Grafana takes ~30-60s to come up.
==============================================================
EOF

if [ "$DNS_LAB" = "true" ]; then
cat <<EOF
 dns-lab: ON — flow-enricher resolves nl6's 10/8 flow IPs via CoreDNS @172.18.0.53.
   In Grafana flows dashboards, src/dst should show nl6-host-10-<b>-<c>-<d>.lab
   hostnames (give the enricher a minute to warm its cache + the MVs to roll up).
EOF
fi
