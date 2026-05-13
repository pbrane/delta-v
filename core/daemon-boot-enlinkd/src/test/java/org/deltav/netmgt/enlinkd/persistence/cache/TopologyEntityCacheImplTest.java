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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;

class TopologyEntityCacheImplTest {

    @Test
    void getNodeTopologyEntitiesReturnsResultFromDao() {
        TopologyEntityDao dao = mock(TopologyEntityDao.class);
        List<NodeTopologyEntity> fixture = Collections.emptyList();
        when(dao.getNodeTopologyEntities()).thenReturn(fixture);

        TopologyEntityCacheImpl cache = new TopologyEntityCacheImpl(dao, 300);

        assertThat(cache.getNodeTopologyEntities()).isSameAs(fixture);
    }

    @Test
    void cacheHitsAvoidRepeatedDaoCalls() {
        TopologyEntityDao dao = mock(TopologyEntityDao.class);
        when(dao.getNodeTopologyEntities()).thenReturn(Collections.emptyList());

        TopologyEntityCacheImpl cache = new TopologyEntityCacheImpl(dao, 300);
        cache.getNodeTopologyEntities();
        cache.getNodeTopologyEntities();
        cache.getNodeTopologyEntities();

        verify(dao, times(1)).getNodeTopologyEntities();
    }

    @Test
    void allElevenGettersDelegateToDao() {
        TopologyEntityDao dao = mock(TopologyEntityDao.class);
        when(dao.getNodeTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getCdpLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIsIsLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getLldpLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getOspfLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getOspfAreaTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getCdpElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIsIsElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getLldpElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getSnmpTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIpTopologyEntities()).thenReturn(Collections.emptyList());

        TopologyEntityCacheImpl cache = new TopologyEntityCacheImpl(dao, 300);

        cache.getNodeTopologyEntities();
        cache.getCdpLinkTopologyEntities();
        cache.getIsIsLinkTopologyEntities();
        cache.getLldpLinkTopologyEntities();
        cache.getOspfLinkTopologyEntities();
        cache.getOspfAreaTopologyEntities();
        cache.getCdpElementTopologyEntities();
        cache.getIsIsElementTopologyEntities();
        cache.getLldpElementTopologyEntities();
        cache.getSnmpInterfaceTopologyEntities();
        cache.getIpInterfaceTopologyEntities();

        verify(dao).getNodeTopologyEntities();
        verify(dao).getCdpLinkTopologyEntities();
        verify(dao).getIsIsLinkTopologyEntities();
        verify(dao).getLldpLinkTopologyEntities();
        verify(dao).getOspfLinkTopologyEntities();
        verify(dao).getOspfAreaTopologyEntities();
        verify(dao).getCdpElementTopologyEntities();
        verify(dao).getIsIsElementTopologyEntities();
        verify(dao).getLldpElementTopologyEntities();
        verify(dao).getSnmpTopologyEntities();
        verify(dao).getIpTopologyEntities();
    }

    @Test
    void refreshReloadsAllElevenCaches() {
        TopologyEntityDao dao = mock(TopologyEntityDao.class);
        when(dao.getNodeTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getCdpLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIsIsLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getLldpLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getOspfLinkTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getOspfAreaTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getCdpElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIsIsElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getLldpElementTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getSnmpTopologyEntities()).thenReturn(Collections.emptyList());
        when(dao.getIpTopologyEntities()).thenReturn(Collections.emptyList());

        TopologyEntityCacheImpl cache = new TopologyEntityCacheImpl(dao, 300);

        // Prime all 11 caches so each has a value to refresh
        cache.getNodeTopologyEntities();
        cache.getCdpLinkTopologyEntities();
        cache.getIsIsLinkTopologyEntities();
        cache.getLldpLinkTopologyEntities();
        cache.getOspfLinkTopologyEntities();
        cache.getOspfAreaTopologyEntities();
        cache.getCdpElementTopologyEntities();
        cache.getIsIsElementTopologyEntities();
        cache.getLldpElementTopologyEntities();
        cache.getSnmpInterfaceTopologyEntities();
        cache.getIpInterfaceTopologyEntities();

        // Each DAO method should have been called exactly once so far
        verify(dao, times(1)).getNodeTopologyEntities();
        verify(dao, times(1)).getCdpLinkTopologyEntities();
        verify(dao, times(1)).getIsIsLinkTopologyEntities();
        verify(dao, times(1)).getLldpLinkTopologyEntities();
        verify(dao, times(1)).getOspfLinkTopologyEntities();
        verify(dao, times(1)).getOspfAreaTopologyEntities();
        verify(dao, times(1)).getCdpElementTopologyEntities();
        verify(dao, times(1)).getIsIsElementTopologyEntities();
        verify(dao, times(1)).getLldpElementTopologyEntities();
        verify(dao, times(1)).getSnmpTopologyEntities();
        verify(dao, times(1)).getIpTopologyEntities();

        cache.refresh();

        // refresh() should trigger one additional DAO call per cache (Guava's default
        // CacheLoader.reload() invokes load() synchronously when refreshAfterWrite is
        // not configured), so we now expect 2 invocations per DAO method.
        verify(dao, times(2)).getNodeTopologyEntities();
        verify(dao, times(2)).getCdpLinkTopologyEntities();
        verify(dao, times(2)).getIsIsLinkTopologyEntities();
        verify(dao, times(2)).getLldpLinkTopologyEntities();
        verify(dao, times(2)).getOspfLinkTopologyEntities();
        verify(dao, times(2)).getOspfAreaTopologyEntities();
        verify(dao, times(2)).getCdpElementTopologyEntities();
        verify(dao, times(2)).getIsIsElementTopologyEntities();
        verify(dao, times(2)).getLldpElementTopologyEntities();
        verify(dao, times(2)).getSnmpTopologyEntities();
        verify(dao, times(2)).getIpTopologyEntities();
    }
}
