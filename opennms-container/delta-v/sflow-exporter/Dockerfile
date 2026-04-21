FROM debian:12-slim

# host-sflow is not packaged for Debian/Ubuntu main repositories and InMon
# only ships amd64 .deb files. We build from source — the build is a plain
# Makefile with minimal dependencies and finishes in ~30 seconds.
#
# libpcap0.8 is listed explicitly (not just as a transitive dep of
# libpcap-dev) so apt marks it as manually-installed. Otherwise the final
# `apt-get purge -y --auto-remove libpcap-dev` step removes it along with
# its build-time sibling, the mod_pcap.so module then dlopens and fails
# with "libpcap.so.0.8: cannot open shared object file" at runtime, and
# hsflowd silently emits counter samples only — no flow samples reach the
# collector. (Regression history: introduced when the Dockerfile was first
# written; caught 2026-04-21 after debugging zero sFlow rows in ClickHouse.)
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        git \
        ca-certificates \
        libpcap-dev \
        libpcap0.8 \
        iproute2 \
        iputils-ping \
        curl \
        bash \
    && git clone --depth=1 https://github.com/sflow/host-sflow.git /tmp/host-sflow \
    && make -C /tmp/host-sflow FEATURES="PCAP" \
    && make -C /tmp/host-sflow install \
    && rm -rf /tmp/host-sflow \
    && DEBIAN_FRONTEND=noninteractive apt-get purge -y --auto-remove \
        build-essential clang git libpcap-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ldd /etc/hsflowd/modules/mod_pcap.so | grep -qE 'libpcap\.so\.[0-9.]+ => /' \
       || (echo "ERROR: mod_pcap.so has an unresolved libpcap runtime dependency:" \
           && ldd /etc/hsflowd/modules/mod_pcap.so && exit 1)

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
