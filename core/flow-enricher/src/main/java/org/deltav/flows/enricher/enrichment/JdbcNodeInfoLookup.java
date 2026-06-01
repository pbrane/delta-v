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
import org.springframework.jdbc.core.RowMapper;

/**
 * JDBC-based node lookup for flow enrichment. Resolves IP addresses and node
 * IDs to {@link NodeInfo} via the OpenNMS {@code node} and {@code ipinterface}
 * tables.
 *
 * <p>Backed by a Caffeine cache to avoid hitting the database on every flow.
 * High-volume flow streams (10k+ flows/sec) routinely repeat the same
 * exporter/src/dst IP addresses; without this cache the lookup would saturate
 * the connection pool. Negative results are cached too — a flow whose src
 * address belongs to no monitored node should not retry the lookup until the
 * TTL expires.
 *
 * <p>Cached values are wrapped in {@link Optional} so the cache can store
 * "looked up, came back empty" without needing a sentinel value.
 */
public class JdbcNodeInfoLookup {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcNodeInfoLookup.class);

    private static final String SQL_BY_IP =
            "SELECT n.nodeid, n.foreignsource, n.foreignid, n.location, n.nodelabel " +
            "FROM node n JOIN ipinterface i ON n.nodeid = i.nodeid " +
            "WHERE i.ipaddr = ? LIMIT 1";

    private static final String SQL_BY_NODE_ID =
            "SELECT nodeid, foreignsource, foreignid, location, nodelabel FROM node WHERE nodeid = ?";

    private static final RowMapper<NodeInfo> ROW_MAPPER = (rs, rowNum) -> new NodeInfo(
            rs.getInt("nodeid"),
            rs.getString("foreignsource"),
            rs.getString("foreignid"),
            rs.getString("location"),
            rs.getString("nodelabel"));

    public record NodeInfo(int nodeId, String foreignSource, String foreignId, String location, String nodeLabel) {}

    private final JdbcTemplate jdbc;
    private final Cache<String, Optional<NodeInfo>> ipCache;
    private final Cache<Integer, Optional<NodeInfo>> nodeIdCache;

    public JdbcNodeInfoLookup(JdbcTemplate jdbc, Duration cacheTtl) {
        this.jdbc = jdbc;
        this.ipCache = Caffeine.newBuilder()
                .expireAfterWrite(cacheTtl)
                .maximumSize(50_000)
                .build();
        this.nodeIdCache = Caffeine.newBuilder()
                .expireAfterWrite(cacheTtl)
                .maximumSize(50_000)
                .build();
    }

    public NodeInfo lookupByIpAddress(String ipAddress) {
        if (ipAddress == null || ipAddress.isEmpty()) {
            return null;
        }
        return ipCache.get(ipAddress, this::queryByIpAddress).orElse(null);
    }

    public NodeInfo lookupByNodeId(int nodeId) {
        return nodeIdCache.get(nodeId, this::queryByNodeId).orElse(null);
    }

    private Optional<NodeInfo> queryByIpAddress(String ipAddress) {
        try {
            List<NodeInfo> results = jdbc.query(SQL_BY_IP, ROW_MAPPER, ipAddress);
            return results.isEmpty() ? Optional.empty() : Optional.of(results.getFirst());
        } catch (Exception e) {
            LOG.debug("Node lookup failed for IP {}: {}", ipAddress, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<NodeInfo> queryByNodeId(int nodeId) {
        try {
            List<NodeInfo> results = jdbc.query(SQL_BY_NODE_ID, ROW_MAPPER, nodeId);
            return results.isEmpty() ? Optional.empty() : Optional.of(results.getFirst());
        } catch (Exception e) {
            LOG.debug("Node lookup failed for nodeId {}: {}", nodeId, e.getMessage());
            return Optional.empty();
        }
    }
}
