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
package org.deltav.flows.enricher;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;

import org.deltav.flows.enricher.classification.ApplicationClassifier;
import org.deltav.flows.enricher.classification.PortBasedApplicationClassifier;
import org.deltav.flows.enricher.enrichment.FlowLocalityCalculator;
import org.deltav.flows.enricher.enrichment.InterfaceMarkingCache;
import org.deltav.flows.enricher.enrichment.JdbcNodeInfoLookup;
import org.deltav.flows.enricher.mapping.FlowToDocumentMapper;
import org.deltav.flows.enricher.parser.DropwizardToPrometheusBridge;
import org.deltav.flows.enricher.parser.LoggingEventForwarder;
import org.deltav.flows.enricher.parser.NoOpDnsResolver;
import org.deltav.flows.enricher.parser.StaticIdentity;
import org.deltav.flows.enricher.parser.ThreadLocalDispatcher;
import org.deltav.flows.enricher.protocol.IpfixMessageProcessor;
import org.deltav.flows.enricher.protocol.Netflow5MessageProcessor;
import org.deltav.flows.enricher.protocol.Netflow9MessageProcessor;
import org.deltav.flows.enricher.protocol.ProtocolMessageProcessor;
import org.deltav.flows.enricher.protocol.SFlowMessageProcessor;
import org.deltav.flows.enricher.protocol.SimpleAdapterDefinition;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.telemetry.listeners.UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.IpfixUdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.Netflow5UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.Netflow9UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.ie.InformationElementDatabase;
import org.opennms.netmgt.telemetry.protocols.sflow.parser.SFlowUdpParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Bean wiring for the flow-enricher service. Beans are constructed with
 * constructor injection rather than field {@code @Autowired} per the
 * delta-v project convention.
 */
@Configuration
public class FlowEnricherConfiguration {

    @Bean
    JdbcTemplate flowEnricherJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    JdbcNodeInfoLookup jdbcNodeInfoLookup(
            JdbcTemplate flowEnricherJdbcTemplate,
            @Value("${deltav.flows.node-lookup.cache-ttl:5m}") Duration cacheTtl) {
        return new JdbcNodeInfoLookup(flowEnricherJdbcTemplate, cacheTtl);
    }

    @Bean
    FlowLocalityCalculator flowLocalityCalculator() {
        return new FlowLocalityCalculator();
    }

    @Bean
    InterfaceMarkingCache interfaceMarkingCache(
            JdbcTemplate flowEnricherJdbcTemplate,
            @Value("${deltav.flows.interface-marking.cache-ttl:24h}") Duration cacheTtl) {
        return new InterfaceMarkingCache(flowEnricherJdbcTemplate, cacheTtl);
    }

    @Bean
    SinkMessageDeserializer sinkMessageDeserializer() {
        return new SinkMessageDeserializer();
    }

    @Bean
    ApplicationClassifier applicationClassifier() {
        return new PortBasedApplicationClassifier();
    }

    @Bean
    FlowToDocumentMapper flowToDocumentMapper() {
        return new FlowToDocumentMapper();
    }

    /**
     * A process-local Dropwizard {@link MetricRegistry} shared across the
     * four horizon flow adapters and parsers. Exposed to Spring Boot Actuator
     * via {@link DropwizardToPrometheusBridge} so parser and adapter metrics
     * are scrapable at {@code /actuator/prometheus}.
     */
    @Bean
    MetricRegistry flowEnricherMetricRegistry() {
        return new MetricRegistry();
    }

    /**
     * Mirrors every metric in the shared Dropwizard {@link MetricRegistry}
     * into Spring Boot's Micrometer composite registry via a
     * {@link com.codahale.metrics.MetricRegistryListener}, making horizon
     * parser and adapter metrics scrapable at {@code /actuator/prometheus}
     * under the {@code flow_enricher_*} prefix.
     */
    @Bean
    DropwizardToPrometheusBridge dropwizardToPrometheusBridge(
            MetricRegistry flowEnricherMetricRegistry) {
        return new DropwizardToPrometheusBridge(flowEnricherMetricRegistry, "flow_enricher");
    }

    /**
     * Single-threaded daemon {@link ScheduledExecutorService} used by the
     * horizon UDP parsers to schedule periodic session/template cleanup work.
     * The {@code destroyMethod = "shutdown"} ensures Spring shuts it down
     * cleanly on context close.
     */
    @Bean(destroyMethod = "shutdown")
    ScheduledExecutorService flowParserSessionCleanup() {
        AtomicLong counter = new AtomicLong(0);
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "flow-parser-session-cleanup-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }

    /**
     * Lifecycle bean that starts and stops the Netflow5, Netflow9, IPFIX, and
     * sFlow UDP parsers as part of the Spring application context lifecycle.
     * Each parser's {@code start()} runs in {@code @PostConstruct} with the
     * shared scheduler, and its {@code stop()} runs on context close.
     */
    @Bean
    ParserLifecycle flowParserLifecycle(
            ScheduledExecutorService flowParserSessionCleanup,
            Netflow5UdpParser netflow5UdpParser,
            Netflow9UdpParser netflow9UdpParser,
            IpfixUdpParser ipfixUdpParser,
            SFlowUdpParser sflowUdpParser) {
        return new ParserLifecycle(
                flowParserSessionCleanup,
                List.of(netflow5UdpParser, netflow9UdpParser, ipfixUdpParser, sflowUdpParser));
    }

    /**
     * Manages the start/stop lifecycle of the horizon UDP parsers. Each
     * parser must be started with a {@link ScheduledExecutorService} so it
     * can schedule periodic session cleanup work (template expiry, etc.).
     */
    public static class ParserLifecycle {

        private static final Logger LOG = LoggerFactory.getLogger(ParserLifecycle.class);

        private final ScheduledExecutorService scheduler;
        private final List<UdpParser> parsers;

        ParserLifecycle(ScheduledExecutorService scheduler, List<UdpParser> parsers) {
            this.scheduler = scheduler;
            this.parsers = List.copyOf(parsers);
        }

        @jakarta.annotation.PostConstruct
        public void start() {
            for (UdpParser parser : parsers) {
                parser.start(scheduler);
                LOG.info("Started horizon parser {}", parser.getName());
            }
        }

        @jakarta.annotation.PreDestroy
        public void stop() {
            for (UdpParser parser : parsers) {
                try {
                    parser.stop();
                    LOG.info("Stopped horizon parser {}", parser.getName());
                } catch (Exception ex) {
                    LOG.warn("Error stopping horizon parser {} — continuing shutdown", parser.getName(), ex);
                }
            }
        }
    }

    /**
     * Singleton {@link ThreadLocalDispatcher} shared by all four protocol
     * processors. Each call installs a private {@link
     * org.deltav.flows.enricher.parser.CapturingDispatcher} per thread for
     * Stage 1 parse capture and clears it in {@code finally}.
     */
    @Bean
    ThreadLocalDispatcher threadLocalDispatcher() {
        return new ThreadLocalDispatcher();
    }

    /**
     * Logging-only {@link EventForwarder} for horizon parser operational
     * events (clock skew, repeated unknown templates, illegal flows). A
     * Kafka-backed forwarder is deferred as a Phase 2 followup.
     */
    @Bean
    EventForwarder flowParserEventForwarder() {
        return new LoggingEventForwarder();
    }

    /**
     * Static {@link org.opennms.distributed.core.api.Identity} for the
     * flow-enricher service. Used by horizon parsers as part of the
     * per-exporter session key.
     */
    @Bean
    StaticIdentity flowParserIdentity(
            @Value("${deltav.flows.parser.system-id:flow-enricher}") String systemId,
            @Value("${deltav.flows.parser.location:Default}") String location) {
        return new StaticIdentity(systemId, location, "flow-enricher");
    }

    /**
     * No-op {@link DnsResolver} used by horizon parsers for reverse-DNS
     * enrichment. The flow-enricher does its own node lookup via JDBC; we
     * do not want the parsers to issue async DNS queries.
     */
    @Bean
    DnsResolver flowParserDnsResolver() {
        return new NoOpDnsResolver();
    }

    /**
     * Standard IPFIX/Netflow9 {@link InformationElementDatabase} loaded
     * from horizon's built-in provider classes. Used by both
     * {@link Netflow9UdpParser} and {@link IpfixUdpParser}.
     */
    @Bean
    InformationElementDatabase informationElementDatabase() {
        return new InformationElementDatabase(
                new org.opennms.netmgt.telemetry.protocols.netflow.parser.ipfix.InformationElementProvider(),
                new org.opennms.netmgt.telemetry.protocols.netflow.parser.netflow9.InformationElementProvider());
    }

    @Bean
    Netflow5UdpParser netflow5UdpParser(
            ThreadLocalDispatcher threadLocalDispatcher,
            EventForwarder flowParserEventForwarder,
            StaticIdentity flowParserIdentity,
            DnsResolver flowParserDnsResolver,
            MetricRegistry flowEnricherMetricRegistry) {
        return new Netflow5UdpParser(
                "Netflow-5",
                threadLocalDispatcher,
                flowParserEventForwarder,
                flowParserIdentity,
                flowParserDnsResolver,
                flowEnricherMetricRegistry);
    }

    @Bean
    Netflow9UdpParser netflow9UdpParser(
            ThreadLocalDispatcher threadLocalDispatcher,
            EventForwarder flowParserEventForwarder,
            StaticIdentity flowParserIdentity,
            DnsResolver flowParserDnsResolver,
            MetricRegistry flowEnricherMetricRegistry,
            InformationElementDatabase informationElementDatabase) {
        return new Netflow9UdpParser(
                "Netflow-9",
                threadLocalDispatcher,
                flowParserEventForwarder,
                flowParserIdentity,
                flowParserDnsResolver,
                flowEnricherMetricRegistry,
                informationElementDatabase);
    }

    @Bean
    IpfixUdpParser ipfixUdpParser(
            ThreadLocalDispatcher threadLocalDispatcher,
            EventForwarder flowParserEventForwarder,
            StaticIdentity flowParserIdentity,
            DnsResolver flowParserDnsResolver,
            MetricRegistry flowEnricherMetricRegistry,
            InformationElementDatabase informationElementDatabase) {
        return new IpfixUdpParser(
                "IPFIX",
                threadLocalDispatcher,
                flowParserEventForwarder,
                flowParserIdentity,
                flowParserDnsResolver,
                flowEnricherMetricRegistry,
                informationElementDatabase);
    }

    /**
     * sFlow UDP parser. Unlike the Netflow5/9/IPFIX parsers, {@link SFlowUdpParser}
     * does not extend horizon's {@code ParserBase} — it implements
     * {@link UdpParser} directly and manages its own internal
     * {@link java.util.concurrent.ThreadPoolExecutor} created from a
     * {@link org.opennms.core.concurrent.LogPreservingThreadFactory}.
     *
     * <p>The delta-v-local shim of {@code LogPreservingThreadFactory} (at
     * {@code core/flow-enricher/src/main/java/org/opennms/core/concurrent/LogPreservingThreadFactory.java})
     * shadows horizon's class by fully-qualified name, so both bean
     * construction and {@link SFlowUdpParser#start(ScheduledExecutorService)}
     * succeed without pulling in the banned {@code opennms-util} module.
     * This is the same shim that unblocks the Netflow parsers (see the
     * {@code feature/flow-enricher-phase2-parser-bridge} merge).
     */
    @Bean
    SFlowUdpParser sflowUdpParser(
            ThreadLocalDispatcher threadLocalDispatcher,
            DnsResolver flowParserDnsResolver) {
        final SFlowUdpParser parser = new SFlowUdpParser(
                "SFlow",
                threadLocalDispatcher,
                flowParserDnsResolver);
        // Defensive: disable reverse-DNS lookups on the sFlow parser.
        // PR #156 fixed a horizon SFlowUdpParser NPE where unresolvable
        // Docker-bridge IPs tripped a DNS code path inside the parser; the
        // short-circuit here keeps that latent path unreachable.
        parser.setDnsLookupsEnabled(false);
        return parser;
    }

    @Bean
    Netflow5MessageProcessor netflow5Processor(
            Netflow5UdpParser netflow5UdpParser,
            MetricRegistry flowEnricherMetricRegistry,
            ThreadLocalDispatcher threadLocalDispatcher) {
        return new Netflow5MessageProcessor(
                netflow5UdpParser,
                new SimpleAdapterDefinition("Netflow-5"),
                flowEnricherMetricRegistry,
                threadLocalDispatcher);
    }

    @Bean
    Netflow9MessageProcessor netflow9Processor(
            Netflow9UdpParser netflow9UdpParser,
            MetricRegistry flowEnricherMetricRegistry,
            ThreadLocalDispatcher threadLocalDispatcher) {
        return new Netflow9MessageProcessor(
                netflow9UdpParser,
                new SimpleAdapterDefinition("Netflow-9"),
                flowEnricherMetricRegistry,
                threadLocalDispatcher);
    }

    @Bean
    IpfixMessageProcessor ipfixProcessor(
            IpfixUdpParser ipfixUdpParser,
            MetricRegistry flowEnricherMetricRegistry,
            ThreadLocalDispatcher threadLocalDispatcher) {
        return new IpfixMessageProcessor(
                ipfixUdpParser,
                new SimpleAdapterDefinition("IPFIX"),
                flowEnricherMetricRegistry,
                threadLocalDispatcher);
    }

    @Bean
    SFlowMessageProcessor sflowProcessor(
            SFlowUdpParser sflowUdpParser,
            MetricRegistry flowEnricherMetricRegistry,
            ThreadLocalDispatcher threadLocalDispatcher) {
        return new SFlowMessageProcessor(
                sflowUdpParser,
                new SimpleAdapterDefinition("SFlow"),
                flowEnricherMetricRegistry,
                threadLocalDispatcher);
    }

    @Bean
    FlowEnrichmentFunction flowEnrichmentFunction(
            SinkMessageDeserializer deserializer,
            JdbcNodeInfoLookup nodeInfoLookup,
            FlowLocalityCalculator localityCalculator,
            InterfaceMarkingCache interfaceMarkingCache,
            ApplicationClassifier applicationClassifier,
            FlowToDocumentMapper flowToDocumentMapper,
            Netflow5MessageProcessor netflow5Processor,
            Netflow9MessageProcessor netflow9Processor,
            IpfixMessageProcessor ipfixProcessor,
            SFlowMessageProcessor sflowProcessor) {

        Map<String, ProtocolMessageProcessor> dispatchMap = Map.of(
                "Telemetry-Netflow-5", netflow5Processor,
                "Telemetry-Netflow-9", netflow9Processor,
                "Telemetry-IPFIX",     ipfixProcessor,
                "Telemetry-SFlow",     sflowProcessor);

        return new FlowEnrichmentFunction(
                deserializer,
                nodeInfoLookup,
                localityCalculator,
                interfaceMarkingCache,
                applicationClassifier,
                flowToDocumentMapper,
                dispatchMap);
    }

    /**
     * Spring Cloud Stream function binding. The bean name {@code enrichFlows}
     * matches the {@code spring.cloud.function.definition} value in
     * application.yml; the suffixes {@code -in-0} / {@code -out-0} are
     * generated automatically by Spring Cloud Function.
     *
     * <p>The splitter signature {@code Function<Message<byte[]>, List<byte[]>>}
     * emits zero or more output records per input record; an empty list
     * discards the message entirely. The {@code Message<byte[]>} envelope
     * carries the Kafka {@code kafka_receivedTopic} header, which
     * {@link FlowEnrichmentFunction} uses to extract the module ID for
     * protocol dispatch.
     */
    @Bean
    Function<Message<byte[]>, List<byte[]>> enrichFlows(FlowEnrichmentFunction enrichmentFunction) {
        return enrichmentFunction::processMessage;
    }

    /**
     * Periodically purges TTL-expired entries from the InterfaceMarkingCache
     * to bound memory growth.
     */
    @Bean
    InterfaceMarkingCacheCleaner interfaceMarkingCacheCleaner(InterfaceMarkingCache cache) {
        return new InterfaceMarkingCacheCleaner(cache);
    }

    /**
     * Trivial bean wrapper that owns the {@link Scheduled} hook. Defining the
     * scheduled method on the {@code @Configuration} class itself is awkward
     * because Spring proxies it; an explicit holder bean keeps the wiring
     * straightforward and easy to mock in tests.
     */
    public static class InterfaceMarkingCacheCleaner {
        private final InterfaceMarkingCache cache;

        InterfaceMarkingCacheCleaner(InterfaceMarkingCache cache) {
            this.cache = cache;
        }

        @Scheduled(fixedRateString = "${deltav.flows.interface-marking.clean-interval-ms:3600000}")
        public void clean() {
            cache.cleanExpired();
        }
    }
}
