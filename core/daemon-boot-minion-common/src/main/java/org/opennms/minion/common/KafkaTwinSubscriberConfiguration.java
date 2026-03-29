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

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.opennms.core.ipc.twin.api.TwinSubscriber;
import org.opennms.core.ipc.twin.kafka.subscriber.KafkaTwinSubscriber;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;

/**
 * Spring Boot {@link Configuration} that wraps the OSGi-designed
 * {@link KafkaTwinSubscriber} in a Spring {@link SmartLifecycle}.
 *
 * <p>Lifecycle phase 100 ensures the Twin subscriber starts <b>first</b>,
 * before the Sink client (phase 200) and the RPC server (phase 300).
 * This allows config sync to complete before message dispatch and
 * request handling begin. Shutdown reverses this order automatically.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.twin.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTwinSubscriberConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTwinSubscriberConfiguration.class);

    @Bean
    public KafkaTwinSubscriber kafkaTwinSubscriber(
            @Value("${opennms.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            MinionIdentity minionIdentity,
            TracerRegistry tracerRegistry) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return new KafkaTwinSubscriber(
                minionIdentity,
                new SpringKafkaConfigProvider(kafkaProps),
                tracerRegistry,
                new MetricRegistry());
    }



    @Bean
    public SmartLifecycle kafkaTwinSubscriberLifecycle(KafkaTwinSubscriber subscriber) {

        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                LOG.info("Starting Kafka Twin subscriber (phase 100)");
                try {
                    subscriber.init();
                    running = true;
                    LOG.info("Kafka Twin subscriber started");
                } catch (Exception e) {
                    LOG.error("Failed to initialize Kafka Twin subscriber", e);
                }
            }

            @Override
            public void stop() {
                LOG.info("Stopping Kafka Twin subscriber (phase 100)");
                try {
                    subscriber.close();
                } catch (IOException e) {
                    LOG.warn("Error closing Kafka Twin subscriber", e);
                }
                running = false;
                LOG.info("Kafka Twin subscriber stopped");
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 100;
            }
        };
    }
}
