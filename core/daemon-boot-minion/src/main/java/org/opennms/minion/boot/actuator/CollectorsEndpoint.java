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
package org.opennms.minion.boot.actuator;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.opennms.netmgt.collection.api.ServiceCollectorRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

/**
 * Custom actuator endpoint at {@code /actuator/collectors} listing all registered service collectors.
 *
 * <p>Replaces the Karaf shell command {@code list-collectors}. The registry is optional
 * since the collector subsystem may be disabled via {@code opennms.minion.collector.enabled=false}.</p>
 */
@Component
@Endpoint(id = "collectors")
public class CollectorsEndpoint {

    private final ServiceCollectorRegistry registry;

    @Autowired(required = false)
    public CollectorsEndpoint(ServiceCollectorRegistry registry) {
        this.registry = registry;
    }

    @ReadOperation
    public Map<String, Object> collectors() {
        if (registry == null) {
            return Map.of(
                "count", 0,
                "classNames", Collections.emptySet()
            );
        }
        Set<String> classNames = registry.getCollectorClassNames();
        return Map.of(
            "count", classNames.size(),
            "classNames", classNames
        );
    }
}
