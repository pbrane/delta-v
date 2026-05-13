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
package org.opennms.netmgt.enlinkd.persistence.impl;

import java.util.Collections;
import java.util.List;

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
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

/**
 * JPA implementation of {@link TopologyEntityDao}. Replaces horizon's
 * {@code TopologyEntityDaoHibernate} which depends on the Spring 3-era
 * {@code spring.orm.hibernate3} package — removed in Spring 5+ and not
 * available in delta-v's Spring 7 / Spring Boot 4 runtime.
 *
 * <p>All queries are read-only JPQL constructor projections. The 11 method
 * bodies are filled in a follow-up commit; this skeleton exists so the
 * Testcontainers IT can wire and fail on empty results.</p>
 */
@Repository
@Transactional(readOnly = true)
public class TopologyEntityDaoJpa implements TopologyEntityDao {

    @PersistenceContext
    private EntityManager em; // populated by JPQL queries in the follow-up commit

    @Override
    public List<NodeTopologyEntity> getNodeTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<CdpLinkTopologyEntity> getCdpLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<IsIsLinkTopologyEntity> getIsIsLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<LldpLinkTopologyEntity> getLldpLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<OspfLinkTopologyEntity> getOspfLinkTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<OspfAreaTopologyEntity> getOspfAreaTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<SnmpInterfaceTopologyEntity> getSnmpTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<IpInterfaceTopologyEntity> getIpTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<CdpElementTopologyEntity> getCdpElementTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<IsIsElementTopologyEntity> getIsIsElementTopologyEntities() {
        return Collections.emptyList();
    }

    @Override
    public List<LldpElementTopologyEntity> getLldpElementTopologyEntities() {
        return Collections.emptyList();
    }
}
