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
package org.deltav.minion.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.provision.dns.client.rpc.DnsLookupClientRpcModule;

/**
 * Verifies the Minion registers the DNS lookup RPC module so provisiond's
 * reverse-DNS lookups (DefaultHostnameResolver) are answered instead of timing
 * out. Regression guard for the Karaf→Spring-Boot migration gap where module id
 * "DNS" had no handler.
 */
class DnsConfigurationTest {

    @Test
    void providesDnsLookupRpcModuleWithIdDNS() {
        DnsLookupClientRpcModule module = new DnsConfiguration().dnsLookupClientRpcModule(64);
        assertNotNull(module, "DnsConfiguration must provide a DnsLookupClientRpcModule bean");
        assertEquals("DNS", module.getId(),
                "Minion must register an RPC module with id 'DNS' for reverse-DNS lookups");
    }
}
