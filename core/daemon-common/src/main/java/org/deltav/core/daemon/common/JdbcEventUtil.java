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
package org.deltav.core.daemon.common;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.opennms.netmgt.eventd.AbstractEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * JDBC-backed {@link org.opennms.netmgt.eventd.EventUtil} for Spring Boot daemons.
 *
 * <p>Replaces the Hibernate-backed {@code EventUtilDaoImpl} that depends on the
 * full DAO layer. This implementation uses direct SQL queries against PostgreSQL,
 * consistent with Delta-V's JDBC-first approach in daemon-common.</p>
 *
 * <p>{@link AbstractEventUtil} provides the {@code expandParms()} template method
 * that calls these lookup methods to resolve {@code %nodelabel%}, {@code %ifalias%},
 * {@code %foreignsource%}, etc. tokens in event descriptions and logmsg.</p>
 */
@Component
public class JdbcEventUtil extends AbstractEventUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEventUtil.class);

    private final JdbcTemplate jdbc;

    public JdbcEventUtil(DataSource dataSource, PlatformTransactionManager transactionManager) {
        super(null, new TransactionTemplate(transactionManager));
        this.jdbc = new JdbcTemplate(dataSource);
    }

    @Override
    public String getNodeLabel(long nodeId) throws SQLException {
        return queryString("SELECT nodelabel FROM node WHERE nodeid = ?", nodeId);
    }

    @Override
    public String getNodeLocation(long nodeId) throws SQLException {
        return queryString("SELECT location FROM node WHERE nodeid = ?", nodeId);
    }

    @Override
    public String getForeignSource(long nodeId) throws SQLException {
        return queryString("SELECT foreignsource FROM node WHERE nodeid = ?", nodeId);
    }

    @Override
    public String getForeignId(long nodeId) throws SQLException {
        return queryString("SELECT foreignid FROM node WHERE nodeid = ?", nodeId);
    }

    @Override
    public String getHostName(int nodeId, String hostip) throws SQLException {
        String hostname = queryString(
                "SELECT iphostname FROM ipinterface WHERE nodeid = ? AND ipaddr = ?",
                nodeId, hostip);
        return hostname != null ? hostname : hostip;
    }

    @Override
    public String getIfAlias(long nodeId, String ipAddr) throws SQLException {
        return queryString(
                "SELECT s.snmpifalias FROM snmpinterface s " +
                "JOIN ipinterface i ON s.id = i.snmpinterfaceid " +
                "WHERE i.nodeid = ? AND i.ipaddr = ?",
                nodeId, ipAddr);
    }

    @Override
    public String getPrimaryInterface(long nodeId) throws SQLException {
        return queryString(
                "SELECT ipaddr FROM ipinterface WHERE nodeid = ? AND issnmpprimary = 'P' LIMIT 1",
                nodeId);
    }

    @Override
    public String getAssetFieldValue(String parm, long nodeId) {
        try {
            // Asset fields are column names on the assets table, joined via node.assetrecordid
            // Validate column name to prevent SQL injection
            if (!parm.matches("[a-zA-Z_]+")) {
                LOG.warn("Invalid asset field name: {}", parm);
                return "";
            }
            String lowerParm = parm.toLowerCase();
            return queryString(
                    "SELECT a." + lowerParm + " FROM assets a " +
                    "JOIN node n ON n.assetrecordid = a.id WHERE n.nodeid = ?",
                    nodeId);
        } catch (Exception e) {
            LOG.debug("Failed to lookup asset field '{}' for node {}: {}", parm, nodeId, e.getMessage());
            return "";
        }
    }

    @Override
    public String getHardwareFieldValue(String parm, long nodeId) {
        // Hardware entity lookup — rarely used, return empty for now
        LOG.debug("Hardware field lookup not implemented: {} for node {}", parm, nodeId);
        return "";
    }

    private String queryString(String sql, Object... args) {
        try {
            return jdbc.query(sql, rs -> rs.next() ? rs.getString(1) : null, args);
        } catch (Exception e) {
            LOG.debug("EventUtil query failed [{}]: {}", sql, e.getMessage());
            return null;
        }
    }
}
