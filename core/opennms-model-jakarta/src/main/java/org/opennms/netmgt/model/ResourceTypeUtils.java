/*
 * Copyright (C) 1999-2024 The OpenNMS Group, Inc.
 * Copyright (C) 2026 BeaconStrategists, Inc. (Modifications)
 *
 * This file is part of OpenNMS(R) / Delta-V.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
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
package org.opennms.netmgt.model;

import java.io.File;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.opennms.netmgt.rrd.RrdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for resource type path resolution and storage configuration.
 *
 * <p>Ported from opennms-model to model-jakarta. Methods referencing
 * OnmsResource/OnmsEntity are omitted — those classes are not in the
 * delta-v classpath and the corresponding methods are only called by
 * horizon's read-path resource DAOs, which delta-v does not wire.</p>
 */
public abstract class ResourceTypeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceTypeUtils.class);

    /**
     * Default folder where RRD files are stored.
     *
     * Working directory defaults to $OPENNMS_HOME, so we can find these in $OPENNMS_HOME/share/rrd.
     */
    public static final File DEFAULT_RRD_ROOT = Paths.get("share", "rrd").toAbsolutePath().toFile();

    /** Directory name of where latency data is stored. */
    public static final String RESPONSE_DIRECTORY = "response";

    /** Directory name of where status data is stored. */
    public static final String STATUS_DIRECTORY = "status";

    /** Directory name of where all other collected data is stored. */
    public static final String SNMP_DIRECTORY = "snmp";

    /** Directory name of where stored-by-foreign-source data is stored. */
    public static final String FOREIGN_SOURCE_DIRECTORY = "fs";

    private static final Pattern s_responseDirectoryPattern = Pattern.compile("^" + RESPONSE_DIRECTORY + ".+$");
    private static final Pattern s_statusDirectoryPattern = Pattern.compile("^" + STATUS_DIRECTORY + ".+$");

    public static boolean isStoreByGroup() {
        return Boolean.getBoolean("org.opennms.rrd.storeByGroup");
    }

    public static boolean isStoreByForeignSource() {
        return Boolean.getBoolean("org.opennms.rrd.storeByForeignSource");
    }

    public static boolean isResponseTime(String relativePath) {
        return s_responseDirectoryPattern.matcher(relativePath).matches();
    }

    public static boolean isStatus(String relativePath) {
        return s_statusDirectoryPattern.matcher(relativePath).matches();
    }

    public static File getRelativeNodeSourceDirectory(String nodeSource) {
        String[] ident = getFsAndFidFromNodeSource(nodeSource);
        return new File(FOREIGN_SOURCE_DIRECTORY, File.separator + ident[0] + File.separator + ident[1]);
    }

    public static String[] getFsAndFidFromNodeSource(String nodeSource) {
        final String[] ident = nodeSource.split(":", 2);
        if (!(ident.length == 2)) {
            LOG.warn("'{}' is not in the format foreignSource:foreignId.", nodeSource);
            throw new IllegalArgumentException("Node definition '" + nodeSource
                    + "' is invalid, it should be in the format: 'foreignSource:foreignId'.");
        }
        return ident;
    }

    /**
     * Retrieves the ResourcePath relative to rrd.base.dir.
     *
     * <p>Horizon's features.timeseries TimeseriesPersister and
     * TimeseriesPersistOperationBuilder invoke this on persist. Without the
     * signature delta-v's inner (in-memory) persister throws NoSuchMethodError
     * every cycle; the Kafka publisher path is unaffected but the inner
     * failure counters tick. Implementation matches horizon's
     * opennms-model/ResourceTypeUtils.getResourcePathWithRepository verbatim.</p>
     */
    public static ResourcePath getResourcePathWithRepository(RrdRepository repository, ResourcePath resource) {
        return ResourcePath.get(ResourcePath.get(repository.getRrdBaseDir().getName()), resource);
    }

    /**
     * Returns the number of elements that correspond to the "node resource" for the given path.
     *
     * @param path resource path
     * @return number of elements or -1 if the given path does not map to a node
     */
    public static int getNumPathElementsToNodeLevel(ResourcePath path) {
        final String[] elements = path.elements();
        if (elements == null) return -1;
        if (elements.length >= 2
                && SNMP_DIRECTORY.equals(elements[0])
                && !FOREIGN_SOURCE_DIRECTORY.equals(elements[1])) {
            return 2;
        } else if (elements.length >= 4
                && SNMP_DIRECTORY.equals(elements[0])
                && FOREIGN_SOURCE_DIRECTORY.equals(elements[1])) {
            return 4;
        }
        return -1;
    }
}
