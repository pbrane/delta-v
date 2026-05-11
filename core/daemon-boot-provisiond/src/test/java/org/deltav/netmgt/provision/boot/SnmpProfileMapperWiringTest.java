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
package org.deltav.netmgt.provision.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.opennms.core.snmp.profile.mapper.impl.SnmpProfileMapperImpl;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.snmp.SnmpProfileMapper;
import org.opennms.netmgt.snmp.proxy.LocationAwareSnmpClient;

/**
 * Wiring-only test for ProvisiondBootConfiguration.snmpProfileMapper.
 * Asserts the bean is the real horizon impl (not the deleted NoOp) and that
 * the empty-profile-list smoke path returns Optional.empty().
 * No Spring context, no DataSource, no Postgres.
 */
class SnmpProfileMapperWiringTest {

    @Test
    void snmpProfileMapperBeanIsRealImplWithAllDepsInjected() {
        FilterDao filterDao = mock(FilterDao.class);
        SnmpAgentConfigFactory configFactory = mock(SnmpAgentConfigFactory.class);
        when(configFactory.getProfiles()).thenReturn(Collections.emptyList());
        LocationAwareSnmpClient snmpClient = mock(LocationAwareSnmpClient.class);

        SnmpProfileMapper mapper = new ProvisiondBootConfiguration()
                .snmpProfileMapper(filterDao, configFactory, snmpClient);

        assertThat(mapper).isInstanceOf(SnmpProfileMapperImpl.class);
    }

    @Test
    void getAgentConfigFromProfilesReturnsEmptyWhenNoProfilesConfigured() throws Exception {
        FilterDao filterDao = mock(FilterDao.class);
        SnmpAgentConfigFactory configFactory = mock(SnmpAgentConfigFactory.class);
        when(configFactory.getProfiles()).thenReturn(Collections.emptyList());
        LocationAwareSnmpClient snmpClient = mock(LocationAwareSnmpClient.class);

        SnmpProfileMapper mapper = new ProvisiondBootConfiguration()
                .snmpProfileMapper(filterDao, configFactory, snmpClient);

        var result = mapper
                .getAgentConfigFromProfiles(InetAddress.getLoopbackAddress(), "Default", null)
                .get();

        assertThat(result).isEmpty();
    }
}
