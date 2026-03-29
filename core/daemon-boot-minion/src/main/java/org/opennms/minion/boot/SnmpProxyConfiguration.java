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

import org.opennms.netmgt.snmp.proxy.common.SnmpProxyRpcModule;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the SNMP proxy RPC module (module ID "SNMP").
 *
 * <p>Executes SNMP GET/SET/WALK requests locally on the Minion using
 * the current SnmpStrategy. Replaces the Karaf blueprint that registered
 * {@code SnmpProxyRpcModule.INSTANCE} into the OSGi service registry.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.snmp.enabled", havingValue = "true", matchIfMissing = true)
public class SnmpProxyConfiguration {

    @Bean
    public SnmpProxyRpcModule snmpProxyRpcModule() {
        return SnmpProxyRpcModule.INSTANCE;
    }
}
