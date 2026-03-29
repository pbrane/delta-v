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

import org.opennms.core.daemon.common.NoOpTracerRegistry;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.distributed.core.api.Identity;
import org.opennms.distributed.core.api.MinionIdentity;
import org.opennms.minion.common.MinionDistPollerDao;
import org.opennms.minion.common.MinionProperties;
import org.opennms.minion.common.SpringMinionIdentity;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Shared infrastructure beans that multiple protocol subsystems depend on.
 *
 * <p>Provides identity, tracing, and DAO beans that were previously wired
 * via OSGi blueprint in the Karaf Minion container.</p>
 */
@Configuration
public class MinionInfraConfiguration {

    @Bean
    @Primary
    public SpringMinionIdentity minionIdentity(MinionProperties properties) {
        return new SpringMinionIdentity(properties);
    }

    @Bean
    public TracerRegistry tracerRegistry() {
        return new NoOpTracerRegistry();
    }

    @Bean
    @Primary
    public DistPollerDao distPollerDao(SpringMinionIdentity identity) {
        return new MinionDistPollerDao(identity);
    }
}
