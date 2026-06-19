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

import org.opennms.netmgt.provision.dns.client.rpc.DnsLookupClientRpcModule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the DNS lookup RPC module (module ID "DNS").
 *
 * <p>provisiond's {@code DefaultHostnameResolver} dispatches reverse-DNS lookups
 * (and forward lookups) to the node's location Minion via this RPC module.
 * Without it the Minion has no handler for module id "DNS", so those RPCs go
 * unanswered: provisiond waits out the RPC timeout (~20s), the empty/timed-out
 * response fails to unmarshal, and it falls back to using the IP as the
 * hostname. At scale (e.g. the 36-node nl6 Clos fabric) the cumulative stalls
 * drop nodes from import cycles and leave SNMP collection partial.</p>
 *
 * <p>The Karaf→Spring-Boot Minion migration re-registers each {@code RpcModule}
 * as an explicit bean (Echo/PingProxy/PingSweep/Poller/Collector/Detector/
 * SnmpProxy); this one was missed. The default thread count mirrors horizon's
 * {@code applicationContext-rpc-dns.xml} (64).</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.dns.enabled", havingValue = "true", matchIfMissing = true)
public class DnsConfiguration {

    @Bean
    public DnsLookupClientRpcModule dnsLookupClientRpcModule(
            @Value("${opennms.minion.dns.thread-count:64}") int threadCount) {
        return new DnsLookupClientRpcModule(threadCount);
    }
}
