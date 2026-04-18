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
package org.deltav.collectd.timeseries;

import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.opennms.netmgt.collection.api.AttributeGroup;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collection.api.ServiceParameters;
import org.opennms.netmgt.rrd.RrdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Feature-flagged configuration for the Kafka Time Series producer. When
 * {@code deltav.timeseries.enabled=true}, publishes one TimeseriesBatch
 * protobuf record per CollectionSet poll to the deltav-timeseries topic.
 * When the flag is false (default), none of the beans are created and the
 * persister chain stays on the existing InMemoryStorage-backed
 * TimeseriesPersisterFactory path.
 */
@Configuration
@ConditionalOnProperty(name = "deltav.timeseries.enabled", havingValue = "true")
public class TimeseriesKafkaPublisherConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesKafkaPublisherConfiguration.class);

    public TimeseriesKafkaPublisherConfiguration() {
        LOG.info("TimeseriesKafkaPublisherConfiguration loaded — @ConditionalOnProperty(deltav.timeseries.enabled=true) matched");
    }

    @Bean
    public CollectionSetToProtobufTranslator collectionSetToProtobufTranslator() {
        return new CollectionSetToProtobufTranslator();
    }

    @Bean
    public TimeseriesKafkaPublisher timeseriesKafkaPublisher(
            StreamBridge streamBridge,
            CollectionSetToProtobufTranslator translator,
            MeterRegistry meterRegistry) {
        return new TimeseriesKafkaPublisher(streamBridge, translator, meterRegistry);
    }

    @Bean
    public NewTopic deltavTimeseriesTopic(
            @Value("${deltav.timeseries.partitions:16}") int partitions,
            @Value("${deltav.timeseries.replication-factor:1}") short replicationFactor,
            @Value("${deltav.timeseries.retention-days:1}") int retentionDays) {
        return TopicBuilder.name("deltav-timeseries")
                .partitions(partitions)
                .replicas(replicationFactor)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG,
                        String.valueOf(Duration.ofDays(retentionDays).toMillis()))
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                .build();
    }

    /**
     * Composite factory that fans out each createPersister() call to both the
     * existing InMemoryStorage-backed TimeseriesPersisterFactory and a fresh
     * TimeseriesKafkaPersister. Declared @Primary so Collectd's constructor
     * resolves to this bean when the feature flag is on.
     */
    @Bean
    @Primary
    public PersisterFactory compositePersisterFactory(
            @Qualifier("timeseriesPersisterFactory") PersisterFactory innerFactory,
            TimeseriesKafkaPublisher publisher) {
        LOG.info("Creating compositePersisterFactory wrapping inner={}@{}",
                innerFactory.getClass().getName(),
                System.identityHashCode(innerFactory));
        return new FanoutPersisterFactory(innerFactory, publisher);
    }

    /**
     * Package-private composite factory. Kept as a static nested class so the
     * public API of this module remains the four files listed in the spec.
     */
    static final class FanoutPersisterFactory implements PersisterFactory {
        private final PersisterFactory innerFactory;
        private final TimeseriesKafkaPublisher publisher;

        FanoutPersisterFactory(PersisterFactory innerFactory, TimeseriesKafkaPublisher publisher) {
            this.innerFactory = innerFactory;
            this.publisher = publisher;
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository),
                    new TimeseriesKafkaPersister(publisher, params));
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository,
                                         boolean dontPersistCounters, boolean forceStoreByGroup,
                                         boolean dontReorderAttributes) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository, dontPersistCounters,
                            forceStoreByGroup, dontReorderAttributes),
                    new TimeseriesKafkaPersister(publisher, params));
        }
    }

    /**
     * Forwards each visitor callback to both delegate persisters, inner first
     * then Kafka. Each delegate is invoked inside its own try/catch so that
     * an exception in one does not prevent the other from running. This
     * matters in practice: horizon's TimeseriesPersister has surfaced
     * transaction-propagation failures in delta-v (MetaTagDataLoader marks
     * a read-only transaction rollback-only on certain DB states), and
     * without isolation a single inner failure would silently swallow every
     * Kafka publish for the affected poll cycle. Exceptions are logged at
     * WARN so they do not disappear without operator signal.
     */
    static final class FanoutPersister implements Persister {
        private static final Logger LOG = LoggerFactory.getLogger(FanoutPersister.class);

        private final Persister innerPersister;
        private final TimeseriesKafkaPersister kafkaPersister;

        FanoutPersister(Persister innerPersister, TimeseriesKafkaPersister kafkaPersister) {
            this.innerPersister = innerPersister;
            this.kafkaPersister = kafkaPersister;
        }

        private void runInner(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                // Catch Throwable (not just RuntimeException) because horizon's
                // AbstractPersister has surfaced LinkageErrors (NoSuchMethodError
                // on ResourceTypeUtils.getResourcePathWithRepository) in delta-v
                // from pre-existing daemon-boot classpath mismatches. Kafka path
                // isolation must hold for all failure modes, not just unchecked
                // exceptions.
                LOG.warn("Inner persister threw during {}; continuing with Kafka path", step, e);
            }
        }

        private void runKafka(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                LOG.warn("Kafka persister threw during {}; continuing", step, e);
            }
        }

        @Override
        public void visitCollectionSet(CollectionSet s) {
            runInner("visitCollectionSet", () -> innerPersister.visitCollectionSet(s));
            runKafka("visitCollectionSet", () -> kafkaPersister.visitCollectionSet(s));
        }

        @Override
        public void visitResource(CollectionResource r) {
            runInner("visitResource", () -> innerPersister.visitResource(r));
            runKafka("visitResource", () -> kafkaPersister.visitResource(r));
        }

        @Override
        public void visitGroup(AttributeGroup g) {
            runInner("visitGroup", () -> innerPersister.visitGroup(g));
            runKafka("visitGroup", () -> kafkaPersister.visitGroup(g));
        }

        @Override
        public void visitAttribute(CollectionAttribute a) {
            runInner("visitAttribute", () -> innerPersister.visitAttribute(a));
            runKafka("visitAttribute", () -> kafkaPersister.visitAttribute(a));
        }

        @Override
        public void completeAttribute(CollectionAttribute a) {
            runInner("completeAttribute", () -> innerPersister.completeAttribute(a));
            runKafka("completeAttribute", () -> kafkaPersister.completeAttribute(a));
        }

        @Override
        public void completeGroup(AttributeGroup g) {
            runInner("completeGroup", () -> innerPersister.completeGroup(g));
            runKafka("completeGroup", () -> kafkaPersister.completeGroup(g));
        }

        @Override
        public void completeResource(CollectionResource r) {
            runInner("completeResource", () -> innerPersister.completeResource(r));
            runKafka("completeResource", () -> kafkaPersister.completeResource(r));
        }

        @Override
        public void completeCollectionSet(CollectionSet s) {
            runInner("completeCollectionSet", () -> innerPersister.completeCollectionSet(s));
            runKafka("completeCollectionSet", () -> kafkaPersister.completeCollectionSet(s));
        }

        @Override
        public void persistNumericAttribute(CollectionAttribute a) {
            runInner("persistNumericAttribute", () -> innerPersister.persistNumericAttribute(a));
            runKafka("persistNumericAttribute", () -> kafkaPersister.persistNumericAttribute(a));
        }

        @Override
        public void persistStringAttribute(CollectionAttribute a) {
            runInner("persistStringAttribute", () -> innerPersister.persistStringAttribute(a));
            runKafka("persistStringAttribute", () -> kafkaPersister.persistStringAttribute(a));
        }
    }
}
