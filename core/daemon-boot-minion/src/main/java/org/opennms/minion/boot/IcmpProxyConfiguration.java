/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.minion.boot;

import org.opennms.netmgt.icmp.PingerFactory;
import org.opennms.netmgt.icmp.best.BestMatchPingerFactory;
import org.opennms.netmgt.icmp.proxy.PingProxyRpcModule;
import org.opennms.netmgt.icmp.proxy.PingSweepRpcModule;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the ICMP proxy RPC modules (module IDs "PING" and "PING-SWEEP").
 *
 * <p>Provides a {@link BestMatchPingerFactory} that auto-detects the best
 * available ICMP implementation (JNI, JNA, or NullPinger) and injects it
 * into both ping modules.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.icmp.enabled", havingValue = "true", matchIfMissing = true)
public class IcmpProxyConfiguration {

    @Bean
    public PingerFactory pingerFactory() {
        return new BestMatchPingerFactory();
    }

    @Bean
    public PingProxyRpcModule pingProxyRpcModule(PingerFactory pingerFactory) {
        PingProxyRpcModule module = new PingProxyRpcModule();
        module.setPingerFactory(pingerFactory);
        return module;
    }

    @Bean
    public PingSweepRpcModule pingSweepRpcModule(PingerFactory pingerFactory) {
        PingSweepRpcModule module = new PingSweepRpcModule();
        module.setPingerFactory(pingerFactory);
        return module;
    }
}
