/*
 * Copyright (C) 1999-2024 The OpenNMS Group, Inc.
 * Copyright (C) 2026 BeaconStrategists, Inc. (Modifications)
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
package org.deltav.netmgt.enlinkd.persistence.cache;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityCache;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Delta-V port of horizon's {@code TopologyEntityCacheImpl}. Wraps the
 * Hibernate-free {@link TopologyEntityDao} with per-entity-type Guava
 * LoadingCaches sharing a single TTL configured at construction time.
 *
 * <p>Cache TTL is supplied via constructor (typically from the
 * {@code deltav.enlinkd.topology-cache.duration-seconds} Spring property),
 * replacing horizon's global system-property lookup.</p>
 */
public class TopologyEntityCacheImpl implements TopologyEntityCache {

    private static final String CACHE_KEY = "CACHE_KEY";

    private final TopologyEntityDao topologyEntityDao;

    private final LoadingCache<String, List<NodeTopologyEntity>> nodeTopologyEntities;
    private final LoadingCache<String, List<CdpLinkTopologyEntity>> cdpLinkTopologyEntities;
    private final LoadingCache<String, List<IsIsLinkTopologyEntity>> isIsLinkTopologyEntities;
    private final LoadingCache<String, List<OspfLinkTopologyEntity>> ospfLinkTopologyEntities;
    private final LoadingCache<String, List<OspfAreaTopologyEntity>> ospfAreaTopologyEntities;
    private final LoadingCache<String, List<LldpLinkTopologyEntity>> lldpLinkTopologyEntities;
    private final LoadingCache<String, List<CdpElementTopologyEntity>> cdpElementTopologyEntities;
    private final LoadingCache<String, List<IsIsElementTopologyEntity>> isIsElementTopologyEntities;
    private final LoadingCache<String, List<LldpElementTopologyEntity>> lldpElementTopologyEntities;
    private final LoadingCache<String, List<SnmpInterfaceTopologyEntity>> snmpInterfaceTopologyEntities;
    private final LoadingCache<String, List<IpInterfaceTopologyEntity>> ipInterfaceTopologyEntities;

    public TopologyEntityCacheImpl(TopologyEntityDao topologyEntityDao, int cacheDurationSeconds) {
        this.topologyEntityDao = topologyEntityDao;
        this.nodeTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getNodeTopologyEntities());
        this.cdpLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getCdpLinkTopologyEntities());
        this.isIsLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getIsIsLinkTopologyEntities());
        this.ospfLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getOspfLinkTopologyEntities());
        this.ospfAreaTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getOspfAreaTopologyEntities());
        this.lldpLinkTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getLldpLinkTopologyEntities());
        this.cdpElementTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getCdpElementTopologyEntities());
        this.isIsElementTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getIsIsElementTopologyEntities());
        this.lldpElementTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getLldpElementTopologyEntities());
        this.snmpInterfaceTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getSnmpTopologyEntities());
        this.ipInterfaceTopologyEntities = createCache(cacheDurationSeconds,
                () -> topologyEntityDao.getIpTopologyEntities());
    }

    private static <KEY, VALUE> LoadingCache<KEY, VALUE> createCache(int ttlSeconds,
                                                                     Supplier<VALUE> entitySupplier) {
        CacheLoader<KEY, VALUE> loader = new CacheLoader<KEY, VALUE>() {
            @Override
            public VALUE load(KEY key) {
                return entitySupplier.get();
            }
        };
        return CacheBuilder
                .newBuilder()
                .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
                .build(loader);
    }

    @Override
    public List<NodeTopologyEntity> getNodeTopologyEntities() {
        return nodeTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<CdpLinkTopologyEntity> getCdpLinkTopologyEntities() {
        return cdpLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<OspfLinkTopologyEntity> getOspfLinkTopologyEntities() {
        return ospfLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<OspfAreaTopologyEntity> getOspfAreaTopologyEntities() {
        return ospfAreaTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<IsIsLinkTopologyEntity> getIsIsLinkTopologyEntities() {
        return isIsLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<LldpLinkTopologyEntity> getLldpLinkTopologyEntities() {
        return lldpLinkTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<CdpElementTopologyEntity> getCdpElementTopologyEntities() {
        return cdpElementTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<IsIsElementTopologyEntity> getIsIsElementTopologyEntities() {
        return isIsElementTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<LldpElementTopologyEntity> getLldpElementTopologyEntities() {
        return lldpElementTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<SnmpInterfaceTopologyEntity> getSnmpInterfaceTopologyEntities() {
        return snmpInterfaceTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public List<IpInterfaceTopologyEntity> getIpInterfaceTopologyEntities() {
        return ipInterfaceTopologyEntities.getUnchecked(CACHE_KEY);
    }

    @Override
    public void refresh() {
        nodeTopologyEntities.refresh(CACHE_KEY);
        cdpLinkTopologyEntities.refresh(CACHE_KEY);
        isIsLinkTopologyEntities.refresh(CACHE_KEY);
        lldpLinkTopologyEntities.refresh(CACHE_KEY);
        ospfLinkTopologyEntities.refresh(CACHE_KEY);
        ospfAreaTopologyEntities.refresh(CACHE_KEY);
        cdpElementTopologyEntities.refresh(CACHE_KEY);
        isIsElementTopologyEntities.refresh(CACHE_KEY);
        lldpElementTopologyEntities.refresh(CACHE_KEY);
        snmpInterfaceTopologyEntities.refresh(CACHE_KEY);
        ipInterfaceTopologyEntities.refresh(CACHE_KEY);
    }
}
