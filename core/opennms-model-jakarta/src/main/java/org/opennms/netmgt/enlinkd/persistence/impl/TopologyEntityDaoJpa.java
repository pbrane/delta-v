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
 * <p>All queries are read-only JPQL constructor projections ported from
 * horizon's {@code TopologyEntityDaoHibernate} (HQL → JPQL).</p>
 */
@Repository
@Transactional(readOnly = true)
public class TopologyEntityDaoJpa implements TopologyEntityDao {

    @PersistenceContext
    private EntityManager em;

    @Override
    public List<NodeTopologyEntity> getNodeTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.NodeTopologyEntity("
                        + "n.id, n.type, n.sysObjectId, n.label, n.location) "
                        + "from org.opennms.netmgt.model.OnmsNode n",
                NodeTopologyEntity.class).getResultList();
    }

    @Override
    public List<CdpLinkTopologyEntity> getCdpLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity("
                        + "l.id, l.node.id, l.cdpCacheIfIndex, l.cdpInterfaceName, "
                        + "l.cdpCacheAddress, l.cdpCacheDeviceId, l.cdpCacheDevicePort) "
                        + "from org.opennms.netmgt.enlinkd.model.CdpLink l",
                CdpLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<IsIsLinkTopologyEntity> getIsIsLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity("
                        + "l.id, l.node.id, l.isisISAdjIndex, l.isisCircIfIndex, "
                        + "l.isisISAdjNeighSysID, l.isisISAdjNeighSNPAAddress) "
                        + "from org.opennms.netmgt.enlinkd.model.IsIsLink l",
                IsIsLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<LldpLinkTopologyEntity> getLldpLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity("
                        + "l.id, l.node.id, l.lldpRemChassisId, l.lldpRemSysname, "
                        + "l.lldpRemPortId, l.lldpRemPortIdSubType, l.lldpRemPortDescr, "
                        + "l.lldpPortId, l.lldpPortIdSubType, l.lldpPortDescr, l.lldpPortIfindex) "
                        + "from org.opennms.netmgt.enlinkd.model.LldpLink l",
                LldpLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<OspfLinkTopologyEntity> getOspfLinkTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity("
                        + "l.id, l.node.id, l.ospfIpAddr, l.ospfIpMask, l.ospfRemIpAddr, "
                        + "l.ospfIfIndex, l.ospfIfAreaId) "
                        + "from org.opennms.netmgt.enlinkd.model.OspfLink l",
                OspfLinkTopologyEntity.class).getResultList();
    }

    @Override
    public List<OspfAreaTopologyEntity> getOspfAreaTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity("
                        + "a.id, a.node.id, a.ospfAreaId, a.ospfAuthType, a.ospfImportAsExtern, "
                        + "a.ospfAreaBdrRtrCount, a.ospfAsBdrRtrCount, a.ospfAreaLsaCount) "
                        + "from org.opennms.netmgt.enlinkd.model.OspfArea a",
                OspfAreaTopologyEntity.class).getResultList();
    }

    @Override
    public List<SnmpInterfaceTopologyEntity> getSnmpTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity("
                        + "i.id, i.ifIndex, i.ifName, i.ifAlias, i.ifSpeed, i.node.id) "
                        + "from org.opennms.netmgt.model.OnmsSnmpInterface i",
                SnmpInterfaceTopologyEntity.class).getResultList();
    }

    @Override
    public List<IpInterfaceTopologyEntity> getIpTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity("
                        + "i.id, i.ipAddress, i.netMask, i.isManaged, i.snmpPrimary, "
                        + "i.node.id, i.snmpInterface.id) "
                        + "from org.opennms.netmgt.model.OnmsIpInterface i",
                IpInterfaceTopologyEntity.class).getResultList();
    }

    @Override
    public List<CdpElementTopologyEntity> getCdpElementTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity("
                        + "e.id, e.cdpGlobalDeviceId, e.node.id) "
                        + "from org.opennms.netmgt.enlinkd.model.CdpElement e",
                CdpElementTopologyEntity.class).getResultList();
    }

    @Override
    public List<IsIsElementTopologyEntity> getIsIsElementTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity("
                        + "e.id, e.isisSysID, e.node.id) "
                        + "from org.opennms.netmgt.enlinkd.model.IsIsElement e",
                IsIsElementTopologyEntity.class).getResultList();
    }

    @Override
    public List<LldpElementTopologyEntity> getLldpElementTopologyEntities() {
        return em.createQuery(
                "select new org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity("
                        + "e.id, e.lldpChassisId, e.lldpSysname, e.node.id) "
                        + "from org.opennms.netmgt.enlinkd.model.LldpElement e",
                LldpElementTopologyEntity.class).getResultList();
    }
}
