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
package org.deltav.netmgt.enlinkd.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.deltav.netmgt.enlinkd.persistence.cache.TopologyEntityCacheImpl;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityCache;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;

/**
 * Wiring-only test for EnlinkdDaemonConfiguration's TopologyEntityCache bean.
 * Asserts the cache is the real delta-v impl (not the deleted anonymous no-op)
 * and that it delegates to the injected DAO. No Spring context, no DataSource,
 * no Postgres. DAO bean wiring is covered in TopologyEntityDaoWiringTest.
 */
class TopologyEntityCacheWiringTest {

    @Test
    void topologyEntityCacheBeanIsDeltavImpl() {
        TopologyEntityDao mockDao = mock(TopologyEntityDao.class);
        TopologyEntityCache cache = new EnlinkdDaemonConfiguration().topologyEntityCache(mockDao, 300);
        assertThat(cache).isInstanceOf(TopologyEntityCacheImpl.class);
    }

    @Test
    void cacheDelegatesToInjectedDao() {
        TopologyEntityDao mockDao = mock(TopologyEntityDao.class);
        when(mockDao.getNodeTopologyEntities()).thenReturn(Collections.emptyList());

        TopologyEntityCache cache = new EnlinkdDaemonConfiguration().topologyEntityCache(mockDao, 300);
        cache.getNodeTopologyEntities();

        verify(mockDao).getNodeTopologyEntities();
    }
}
