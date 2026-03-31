/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.netmgt.config.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.opennms.netmgt.config.filter.Column;
import org.opennms.netmgt.config.filter.DatabaseSchema;
import org.opennms.netmgt.config.filter.Join;
import org.opennms.netmgt.config.filter.Table;
import org.opennms.netmgt.filter.api.FilterParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link DatabaseSchemaConfig} that works with
 * a pre-loaded {@link DatabaseSchema} model object.
 *
 * <p>This class replaces the need for daemon-boot modules to depend on
 * {@code DatabaseSchemaConfigFactory} in opennms-config. The schema model
 * can be loaded via Jackson XmlMapper or any other means, then passed to
 * this constructor.</p>
 *
 * <p>Instances are immutable after construction — the join graph is computed
 * once in the constructor and never modified.</p>
 */
public class DefaultDatabaseSchemaConfig implements DatabaseSchemaConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseSchemaConfig.class);

    private final DatabaseSchema config;
    private final Map<String, Join> primaryJoins;

    public DefaultDatabaseSchemaConfig(final DatabaseSchema config) {
        this.config = config;
        this.primaryJoins = buildPrimaryJoins(config);
    }

    private Map<String, Join> buildPrimaryJoins(final DatabaseSchema schema) {
        final Map<String, Join> joins = new ConcurrentHashMap<>();
        final Table primaryTable = findPrimaryTable(schema);
        if (primaryTable == null) {
            LOG.warn("No primary table found in database-schema configuration");
            return joins;
        }

        Set<String> joinableSet = new HashSet<>();
        joinableSet.add(primaryTable.getName());

        int joinableCount = 0;
        while (joinableCount < joinableSet.size()) {
            joinableCount = joinableSet.size();
            final Set<String> newSet = new HashSet<>(joinableSet);
            for (final Table t : schema.getTables()) {
                if (!joinableSet.contains(t.getName())
                        && (t.getVisible() == null || t.getVisible().equalsIgnoreCase("true"))) {
                    for (final Join j : t.getJoins()) {
                        if (joinableSet.contains(j.getTable())) {
                            newSet.add(t.getName());
                            joins.put(t.getName(), j);
                        }
                    }
                }
            }
            joinableSet = newSet;
        }
        return joins;
    }

    private static Table findPrimaryTable(final DatabaseSchema schema) {
        for (final Table t : schema.getTables()) {
            if ((t.getVisible() == null || t.getVisible().equalsIgnoreCase("true"))
                    && t.getKey() != null && t.getKey().equals("primary")) {
                return t;
            }
        }
        return null;
    }

    @Override
    public DatabaseSchema getDatabaseSchema() {
        return config;
    }

    @Override
    public Table getPrimaryTable() {
        return findPrimaryTable(config);
    }

    @Override
    public Table getTableByName(final String name) {
        for (final Table t : config.getTables()) {
            if ((t.getVisible() == null || t.getVisible().equalsIgnoreCase("true"))
                    && t.getName() != null && t.getName().equals(name)) {
                return t;
            }
        }
        return null;
    }

    @Override
    public Table findTableByVisibleColumn(final String colName) {
        for (final Table t : config.getTables()) {
            for (final Column col : t.getColumns()) {
                if ((col.getVisible() == null || col.getVisible().equalsIgnoreCase("true"))
                        && col.getName().equalsIgnoreCase(colName)) {
                    return t;
                }
            }
        }
        return null;
    }

    @Override
    public int getTableCount() {
        return config.getTables().size();
    }

    @Override
    public List<String> getJoinTables(final List<Table> tables) {
        final List<String> joinedTables = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++) {
            final int insertPosition = joinedTables.size();
            String currentTable = tables.get(i).getName();
            while (currentTable != null && !joinedTables.contains(currentTable)) {
                joinedTables.add(insertPosition, currentTable);
                final Join next = primaryJoins.get(currentTable);
                if (next != null) {
                    currentTable = next.getTable();
                } else {
                    currentTable = null;
                }
            }
        }
        return joinedTables;
    }

    @Override
    public String constructJoinExprForTables(final List<Table> tables) {
        final StringBuilder joinExpr = new StringBuilder();
        final List<String> joinTables = getJoinTables(tables);
        if (joinTables.isEmpty()) {
            return "";
        }
        joinExpr.append(joinTables.get(0));
        for (int i = 1; i < joinTables.size(); i++) {
            final Join currentJoin = primaryJoins.get(joinTables.get(i));
            if (currentJoin.getType() != null && !currentJoin.getType().equalsIgnoreCase("inner")) {
                joinExpr.append(" ").append(currentJoin.getType().toUpperCase());
            }
            joinExpr.append(" JOIN ").append(joinTables.get(i)).append(" ON (");
            joinExpr.append(currentJoin.getTable()).append(".").append(currentJoin.getTableColumn()).append(" = ");
            joinExpr.append(joinTables.get(i)).append(".").append(currentJoin.getColumn()).append(")");
        }
        return "FROM " + joinExpr;
    }

    @Override
    public String addColumn(final List<Table> tables, final String column) throws FilterParseException {
        final Table table = findTableByVisibleColumn(column);
        if (table == null) {
            throw new FilterParseException("Could not find the column '" + column + "' in filter rule");
        }
        if (!tables.contains(table)) {
            tables.add(table);
        }
        return table.getName() + "." + column;
    }
}
