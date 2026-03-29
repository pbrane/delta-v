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
package org.opennms.minion.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opennms.core.rpc.api.RpcModule;
import org.springframework.stereotype.Component;

/**
 * Collects all {@link RpcModule} beans and indexes them by module ID.
 *
 * <p>In the Karaf/OSGi world, {@code RpcModule} implementations were
 * discovered dynamically via {@code <reference-list>} with bind/unbind
 * callbacks. In Spring Boot, all {@code RpcModule} beans are injected
 * as a {@code List} and indexed once at construction time.</p>
 */
@Component
public class RpcModuleRegistry {

    private final Map<String, RpcModule<?, ?>> modules;

    @SuppressWarnings("rawtypes")
    public RpcModuleRegistry(List<RpcModule> modules) {
        Map<String, RpcModule<?, ?>> map = new HashMap<>();
        for (RpcModule module : modules) {
            map.put(module.getId(), module);
        }
        this.modules = map;
    }

    public Optional<RpcModule<?, ?>> getModule(String id) {
        return Optional.ofNullable(modules.get(id));
    }

    public Set<String> getModuleIds() {
        return Collections.unmodifiableSet(modules.keySet());
    }
}
