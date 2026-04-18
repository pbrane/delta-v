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

import io.micrometer.core.instrument.Counter;
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
            TimeseriesKafkaPublisher publisher,
            MeterRegistry meterRegistry,
            @Value("${deltav.collectd.persister.inner.fail-fast:false}") boolean failFastInner,
            @Value("${deltav.collectd.persister.kafka.fail-fast:false}") boolean failFastKafka) {
        LOG.info("Creating compositePersisterFactory wrapping inner={}@{} (failFastInner={}, failFastKafka={})",
                innerFactory.getClass().getName(),
                System.identityHashCode(innerFactory),
                failFastInner, failFastKafka);
        return new FanoutPersisterFactory(innerFactory, publisher, meterRegistry,
                failFastInner, failFastKafka);
    }

    /**
     * Package-private composite factory. Kept as a static nested class so the
     * public API of this module remains the four files listed in the spec.
     */
    static final class FanoutPersisterFactory implements PersisterFactory {
        private final PersisterFactory innerFactory;
        private final TimeseriesKafkaPublisher publisher;
        private final MeterRegistry meterRegistry;
        private final boolean failFastInner;
        private final boolean failFastKafka;

        FanoutPersisterFactory(PersisterFactory innerFactory, TimeseriesKafkaPublisher publisher,
                               MeterRegistry meterRegistry,
                               boolean failFastInner, boolean failFastKafka) {
            this.innerFactory = innerFactory;
            this.publisher = publisher;
            this.meterRegistry = meterRegistry;
            this.failFastInner = failFastInner;
            this.failFastKafka = failFastKafka;
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository),
                    new TimeseriesKafkaPersister(publisher, params),
                    meterRegistry, failFastInner, failFastKafka);
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository,
                                         boolean dontPersistCounters, boolean forceStoreByGroup,
                                         boolean dontReorderAttributes) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository, dontPersistCounters,
                            forceStoreByGroup, dontReorderAttributes),
                    new TimeseriesKafkaPersister(publisher, params),
                    meterRegistry, failFastInner, failFastKafka);
        }
    }

    /**
     * Forwards each visitor callback to both delegate persisters, inner first
     * then Kafka. Each delegate is invoked inside its own try/catch so that
     * an exception in one does not prevent the other from running.
     *
     * <p>Observability (added 2026-04-18 — see project_collectd_publisher_inert_investigation):
     * <ul>
     *   <li>{@code deltav.collectd.persister.inner.failures{step=<visitor-step>}}
     *       — Micrometer counter, pre-registered for all 10 visitor steps at
     *       construction so ops can alert on rate&gt;0 rather than missing-metric.</li>
     *   <li>{@code deltav.collectd.persister.kafka.failures{step=<visitor-step>}}
     *       — symmetric for the Kafka side.</li>
     * </ul>
     *
     * <p>Fail-fast toggles (default false in production; CI/IT profiles enable):
     * <ul>
     *   <li>{@code deltav.collectd.persister.inner.fail-fast} — when true, inner
     *       Throwable propagates as RuntimeException instead of being swallowed.</li>
     *   <li>{@code deltav.collectd.persister.kafka.fail-fast} — symmetric.</li>
     * </ul>
     *
     * <p>Phase 0 horizon-side inner-persister bugs (UnexpectedRollbackException
     * in MetaTagDataLoader; NPE in TimeseriesPersistOperationBuilder.setAttributeValue;
     * ClassCastException in TimeseriesPersister.getUserDefinedMetaTags) are still
     * caught and WARN-logged here. They do not block the Kafka path. See memory
     * project_phase0_inner_persister_bugs_followup for the dedicated fix queue.
     */
    static final class FanoutPersister implements Persister {
        private static final String INNER_FAILURES_METER =
                "deltav.collectd.persister.inner.failures";
        private static final String KAFKA_FAILURES_METER =
                "deltav.collectd.persister.kafka.failures";
        private static final String[] STEPS = new String[]{
                "visitCollectionSet", "visitResource", "visitGroup", "visitAttribute",
                "completeAttribute", "completeGroup", "completeResource", "completeCollectionSet",
                "persistNumericAttribute", "persistStringAttribute"
        };

        private static final Logger LOG = LoggerFactory.getLogger(FanoutPersister.class);

        private final Persister innerPersister;
        private final TimeseriesKafkaPersister kafkaPersister;
        private final MeterRegistry meterRegistry;
        private final boolean failFastInner;
        private final boolean failFastKafka;

        FanoutPersister(Persister innerPersister, TimeseriesKafkaPersister kafkaPersister,
                        MeterRegistry meterRegistry, boolean failFastInner, boolean failFastKafka) {
            this.innerPersister = innerPersister;
            this.kafkaPersister = kafkaPersister;
            this.meterRegistry = meterRegistry;
            this.failFastInner = failFastInner;
            this.failFastKafka = failFastKafka;
            preRegisterCounters();
        }

        private void preRegisterCounters() {
            // Pre-register all 20 (2 sides × 10 steps) counter tag combinations at
            // construction time so ops can alert on rate>0 rather than missing-metric
            // (which would otherwise be ambiguous with "no failures occurred").
            for (String step : STEPS) {
                Counter.builder(INNER_FAILURES_METER).tag("step", step).register(meterRegistry);
                Counter.builder(KAFKA_FAILURES_METER).tag("step", step).register(meterRegistry);
            }
        }

        private void runInner(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                meterRegistry.counter(INNER_FAILURES_METER, "step", step).increment();
                LOG.warn("Inner persister threw during {}; continuing with Kafka path", step, e);
                if (failFastInner) {
                    throw new RuntimeException(
                            "Inner persister failed during " + step + " (fail-fast enabled)", e);
                }
            }
        }

        private void runKafka(String step, Runnable task) {
            try {
                task.run();
            } catch (Throwable e) {
                meterRegistry.counter(KAFKA_FAILURES_METER, "step", step).increment();
                LOG.warn("Kafka persister threw during {}; continuing", step, e);
                if (failFastKafka) {
                    throw new RuntimeException(
                            "Kafka persister failed during " + step + " (fail-fast enabled)", e);
                }
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
