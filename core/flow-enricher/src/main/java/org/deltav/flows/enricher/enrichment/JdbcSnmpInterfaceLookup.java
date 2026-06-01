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
package org.deltav.flows.enricher.enrichment;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * JDBC-based resolver for SNMP interface names. Maps a flow's exporter
 * {@code (nodeId, ifIndex)} to {@code snmpinterface.snmpifname}.
 *
 * <p>Backed by a Caffeine cache keyed by {@link IfKey}; negative results are
 * cached as {@link Optional#empty()} so unprovisioned interfaces do not retry
 * the database on every flow. Mirrors {@link JdbcNodeInfoLookup}.
 */
public class JdbcSnmpInterfaceLookup {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSnmpInterfaceLookup.class);

    private static final String SQL =
            "SELECT snmpifname FROM snmpinterface WHERE nodeid = ? AND snmpifindex = ? LIMIT 1";

    /** Cache key: exporter node id + SNMP ifIndex. */
    public record IfKey(int nodeId, int ifIndex) {}

    private final JdbcTemplate jdbc;
    private final Cache<IfKey, Optional<String>> cache;

    public JdbcSnmpInterfaceLookup(JdbcTemplate jdbc, Duration cacheTtl) {
        this.jdbc = jdbc;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(cacheTtl)
                .maximumSize(50_000)
                .build();
    }

    /**
     * @return the SNMP interface name, or {@code null} when the node is
     *         unresolved ({@code nodeId <= 0}), the interface is unknown, or
     *         the name is empty.
     */
    public String lookupIfName(int nodeId, int ifIndex) {
        if (nodeId <= 0) {
            return null;
        }
        return cache.get(new IfKey(nodeId, ifIndex), this::query).orElse(null);
    }

    private Optional<String> query(IfKey key) {
        try {
            List<String> results = jdbc.queryForList(SQL, String.class, key.nodeId(), key.ifIndex());
            if (results.isEmpty()) {
                return Optional.empty();
            }
            String name = results.getFirst();
            return (name == null || name.isEmpty()) ? Optional.empty() : Optional.of(name);
        } catch (Exception e) {
            LOG.debug("Interface name lookup failed for node {} ifindex {}: {}",
                    key.nodeId(), key.ifIndex(), e.getMessage());
            return Optional.empty();
        }
    }
}
