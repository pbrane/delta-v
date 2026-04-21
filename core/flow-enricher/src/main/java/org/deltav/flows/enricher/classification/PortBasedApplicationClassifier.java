/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.flows.enricher.classification;

import java.util.Map;

/**
 * An {@link ApplicationClassifier} that maps well-known IANA port numbers to
 * their canonical application name. The destination port is checked first;
 * if it is not a well-known port, the source port is checked (to classify
 * the return leg of a bidirectional conversation). If neither port matches,
 * the result is {@code "unknown"}.
 *
 * <p>The mapping intentionally covers only the most common services — a
 * more comprehensive classifier can be added in a later phase via a
 * configurable rules engine. This implementation is stateless and
 * thread-safe.
 */
public class PortBasedApplicationClassifier implements ApplicationClassifier {

    private static final String UNKNOWN = "unknown";

    private static final Map<Integer, String> WELL_KNOWN_PORTS = Map.ofEntries(
            // Standard IANA application ports
            Map.entry(20, "FTP-data"),
            Map.entry(21, "FTP"),
            Map.entry(22, "SSH"),
            Map.entry(23, "Telnet"),
            Map.entry(25, "SMTP"),
            Map.entry(53, "DNS"),
            Map.entry(67, "DHCP"),
            Map.entry(68, "DHCP"),
            Map.entry(69, "TFTP"),
            Map.entry(80, "HTTP"),
            Map.entry(110, "POP3"),
            Map.entry(123, "NTP"),
            Map.entry(143, "IMAP"),
            Map.entry(161, "SNMP"),
            Map.entry(162, "SNMP-trap"),
            Map.entry(179, "BGP"),
            Map.entry(389, "LDAP"),
            Map.entry(443, "HTTPS"),
            Map.entry(445, "SMB"),
            Map.entry(465, "SMTPS"),
            Map.entry(514, "Syslog"),
            Map.entry(587, "SMTP"),
            Map.entry(636, "LDAPS"),
            Map.entry(993, "IMAPS"),
            Map.entry(995, "POP3S"),
            Map.entry(1433, "MSSQL"),
            Map.entry(1521, "Oracle"),
            // Delta-V / Minion ports (high ports because Minion runs as non-root)
            Map.entry(1162, "SNMP-trap"),
            Map.entry(1514, "Syslog"),
            // Datacenter storage + RDMA + tunneling
            Map.entry(2049, "NFS"),
            Map.entry(3260, "iSCSI"),
            Map.entry(3306, "MySQL"),
            Map.entry(3389, "RDP"),
            Map.entry(4729, "Flow-telemetry"),
            Map.entry(4789, "VXLAN"),
            Map.entry(4791, "RoCE-v2"),
            Map.entry(5432, "PostgreSQL"),
            Map.entry(5672, "AMQP"),
            Map.entry(6379, "Redis"),
            Map.entry(8080, "HTTP-alt"),
            Map.entry(8443, "HTTPS-alt"),
            Map.entry(9092, "Kafka"),
            Map.entry(9200, "Elasticsearch"),
            Map.entry(27017, "MongoDB"));

    @Override
    public String classify(int dstPort, int srcPort, int protocol) {
        final String byDst = WELL_KNOWN_PORTS.get(dstPort);
        if (byDst != null) {
            return byDst;
        }
        final String bySrc = WELL_KNOWN_PORTS.get(srcPort);
        if (bySrc != null) {
            return bySrc;
        }
        return UNKNOWN;
    }
}
