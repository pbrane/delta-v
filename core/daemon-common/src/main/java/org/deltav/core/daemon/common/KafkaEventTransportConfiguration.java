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
package org.deltav.core.daemon.common;

import org.deltav.core.event.forwarder.kafka.KafkaEventForwarder;
import org.deltav.core.event.forwarder.kafka.KafkaEventForwarderFactory;
import org.deltav.core.event.forwarder.kafka.KafkaEventIpcManagerAdapter;
import org.deltav.core.event.forwarder.kafka.KafkaEventSubscriptionService;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring {@link Configuration} that replaces the OSGi Blueprint
 * {@code blueprint-event-forwarder-kafka.xml} for Spring Boot daemon containers.
 *
 * <p>Creates the Kafka-backed event transport stack:</p>
 * <ol>
 *   <li>{@link KafkaEventForwarder} — publishes events to Kafka topics</li>
 *   <li>{@link KafkaEventSubscriptionService} — consumes events from Kafka topics
 *       and dispatches to registered listeners</li>
 *   <li>{@link KafkaEventIpcManagerAdapter} — composes forwarder + subscription
 *       into the {@link EventIpcManager} interface expected by daemon code</li>
 * </ol>
 *
 * <p>The forwarder's event expander uses {@code NoOpEventProcessor} (the legacy Eventd
 * expansion pipeline is not available). Instead, eventconf enrichment (alarm-data,
 * severity, reduction-key expansion) is handled by {@link EventConfEnrichmentService}
 * which loads event configurations from the database. This enrichment is wired into
 * the forwarder via {@code setEventConfDao()} so ALL events from ALL daemons are
 * enriched before reaching Kafka.</p>
 */
@Configuration
public class KafkaEventTransportConfiguration {

    @Value("${opennms.kafka.bootstrap-servers:kafka:9092}")
    private String bootstrapServers;

    @Value("${opennms.kafka.event-topic:deltav-fault-events}")
    private String eventTopic;

    @Value("${opennms.kafka.ipc-topic:deltav-ipc-events}")
    private String ipcTopic;

    @Value("${opennms.kafka.consumer-group:opennms-core}")
    private String consumerGroup;

    @Value("${opennms.kafka.poll-timeout-ms:100}")
    private long pollTimeoutMs;

    @Bean
    public KafkaEventForwarder kafkaEventForwarder() {
        KafkaEventForwarder forwarder = KafkaEventForwarderFactory.create(bootstrapServers, eventTopic);
        forwarder.setIpcTopicName(ipcTopic);
        return forwarder;
    }

    /**
     * Wires EventConfDao into KafkaEventForwarder after all beans are created.
     * This avoids bean creation order issues — EventConfEnrichmentService needs
     * DataSource which may not be available when KafkaEventForwarder is created.
     */
    @Bean
    public SmartLifecycle eventConfEnrichmentWiring(
            KafkaEventForwarder forwarder,
            @org.springframework.beans.factory.annotation.Autowired(required = false)
            EventConfEnrichmentService eventConfEnrichmentService) {
        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                if (eventConfEnrichmentService != null) {
                    forwarder.setEventConfDao(eventConfEnrichmentService.getEventConfDao());
                }
                running = true;
            }

            @Override public void stop() { running = false; }
            @Override public boolean isRunning() { return running; }
            @Override public int getPhase() { return -20; } // before event consumer starts at -10
        };
    }

    @Bean(destroyMethod = "stop")
    public KafkaEventSubscriptionService kafkaEventSubscriptionService() {
        return KafkaEventSubscriptionService.create(
                bootstrapServers,
                consumerGroup,
                eventTopic + "," + ipcTopic,
                pollTimeoutMs);
    }

    @Bean
    @org.springframework.context.annotation.Primary
    public EventIpcManager eventIpcManager(KafkaEventForwarder forwarder,
                                           KafkaEventSubscriptionService subscriptionService,
                                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                                           EventConfEnrichmentService eventConfEnrichmentService) {
        EventIpcManager base = new KafkaEventIpcManagerAdapter(forwarder, subscriptionService);
        if (eventConfEnrichmentService != null) {
            return new EventIpcManagerEnrichingWrapper(base, eventConfEnrichmentService);
        }
        return base;
    }

    /**
     * Starts the Kafka event consumer AFTER all InitializingBean callbacks
     * have fired (i.e., after AnnotationBasedEventListenerAdapter has
     * registered its listeners). SmartLifecycle runs after bean init but
     * before the application is considered started. Phase -10 ensures this
     * fires before daemon SmartLifecycles at the default phase (0).
     */
    @Bean
    public SmartLifecycle kafkaEventSubscriptionLifecycle(KafkaEventSubscriptionService subscriptionService) {
        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                subscriptionService.start();
                running = true;
            }

            @Override
            public void stop() {
                running = false;
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return -10;
            }
        };
    }
}
