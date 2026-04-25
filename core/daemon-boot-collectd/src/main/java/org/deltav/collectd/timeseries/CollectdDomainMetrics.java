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
package org.deltav.collectd.timeseries;

/**
 * Canonical Micrometer meter names for collectd domain signals. Names become
 * {@code deltav_collectd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>These counters describe the *shape* of horizon's collection-set walk
 * passing through the Kafka-side persister: how many resources, groups, and
 * attributes a single CollectionSet contains. They sit alongside the existing
 * {@code deltav_timeseries_*} batch-level counters from PR #170 (which describe
 * Kafka publish behavior) and the {@code deltav_collectd_persister_*_failures}
 * counters in {@code FanoutPersister} (which describe persister failures).
 *
 * <p>No labels — collection-package and node identity carry too much
 * cardinality at the per-attribute level. The TimeseriesKafkaPublisher
 * already records publish-side counters with bounded tags.
 */
public final class CollectdDomainMetrics {

    private CollectdDomainMetrics() {}

    public static final String COLLECTION_SETS_VISITED   = "deltav.collectd.collection.sets.visited";
    public static final String COLLECTION_RESOURCES      = "deltav.collectd.collection.resources.visited";
    public static final String COLLECTION_GROUPS         = "deltav.collectd.collection.groups.visited";
    public static final String COLLECTION_ATTRIBUTES     = "deltav.collectd.collection.attributes.visited";
    public static final String COLLECTION_SETS_COMPLETED = "deltav.collectd.collection.sets.completed";
}
