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

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;
import org.opennms.netmgt.enlinkd.persistence.impl.TopologyEntityDaoJpa;

/**
 * Wiring-only test for EnlinkdJpaConfiguration's TopologyEntityDaoJpa bean.
 * Asserts the bean is the real JPA impl. No Spring context, no DataSource, no Postgres.
 */
class TopologyEntityDaoWiringTest {

    @Test
    void topologyEntityDaoBeanIsRealJpaImpl() {
        TopologyEntityDao dao = new EnlinkdJpaConfiguration().topologyEntityDaoJpa();
        assertThat(dao).isInstanceOf(TopologyEntityDaoJpa.class);
    }
}
