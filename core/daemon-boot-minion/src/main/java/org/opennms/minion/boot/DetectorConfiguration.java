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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.opennms.core.daemon.common.registry.LocalServiceDetectorRegistry;
import org.opennms.netmgt.provision.detector.client.rpc.DetectorClientRpcModule;
import org.opennms.netmgt.provision.detector.registry.api.ServiceDetectorRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the Detector RPC module (module ID "Detect") and its
 * {@link ServiceDetectorRegistry}.
 *
 * <p>Uses {@link LocalServiceDetectorRegistry} which discovers
 * {@link org.opennms.netmgt.provision.ServiceDetectorFactory} implementations
 * via {@link java.util.ServiceLoader}.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.detector.enabled", havingValue = "true", matchIfMissing = true)
public class DetectorConfiguration {

    @Bean
    public ServiceDetectorRegistry serviceDetectorRegistry() {
        return new LocalServiceDetectorRegistry();
    }

    @Bean
    public Executor scanExecutor() {
        return Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "detector-rpc");
            t.setDaemon(true);
            return t;
        });
    }

    @Bean
    public DetectorClientRpcModule detectorClientRpcModule(ServiceDetectorRegistry serviceDetectorRegistry,
                                                            @org.springframework.beans.factory.annotation.Qualifier("scanExecutor") Executor scanExecutor) {
        DetectorClientRpcModule module = new DetectorClientRpcModule();
        module.setServiceDetectorRegistry(serviceDetectorRegistry);
        module.setExecutor(scanExecutor);
        return module;
    }
}
