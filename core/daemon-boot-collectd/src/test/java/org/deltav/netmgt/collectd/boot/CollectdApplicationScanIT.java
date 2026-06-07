/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * Licensed under the GNU Affero General Public License v3.
 */
package org.deltav.netmgt.collectd.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.admin.NewTopic;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisher;
import org.deltav.core.event.forwarder.kafka.KafkaEventForwarder;
import org.deltav.core.event.forwarder.kafka.KafkaEventSubscriptionService;
import org.junit.jupiter.api.Test;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.rpc.api.RpcClientFactory;
import org.opennms.netmgt.collection.api.LocationAwareCollectorClient;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collectd.Collectd;
import org.opennms.netmgt.collection.api.ServiceCollectorRegistry;
import org.opennms.netmgt.dao.api.IpInterfaceDao;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.filter.api.FilterDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Real-main-class integration test. Boots {@link CollectdApplication} via
 * {@code SpringApplication.run()} semantics (using
 * {@code @SpringBootTest(classes = CollectdApplication.class)}) with
 * {@code deltav.timeseries.enabled=true} and fail-fast toggles on. Asserts
 * the composite PersisterFactory wires correctly so horizon's
 * {@code CollectableService} resolves to our {@code FanoutPersisterFactory},
 * not the bare inner factory.
 *
 * <p>Phase 2 PR #174 scar-prophylactic lineage: real-main-class startup via
 * {@code @SpringBootTest(classes = CollectdApplication.class)} catches
 * {@code @Configuration} class-name vs {@code @Bean} method-name collisions
 * and bean-wiring bugs that unit tests + {@code @Import}-based ITs all
 * bypass. Mandatory {@code commons-io:2.18.0} test dep added per
 * {@code feedback_boot4_testcontainers_commons_io} memory.
 *
 * <p><b>Mocking strategy (adapted from {@code NodeContextProducerSpringContextIT}):</b>
 * {@code CollectdDaemonConfiguration} reads {@code collectd-configuration.xml},
 * {@code snmp-config.xml}, and {@code datacollection-config.xml} from
 * {@code /opt/deltav/etc/} at bean-creation time; the JPA layer requires a
 * live Postgres and the event transport opens a Kafka producer. We mock out
 * the filesystem-reading / infrastructure beans so the scan completes and
 * the flag-gated {@link org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration}
 * wires into the context alongside all of {@link CollectdApplication}'s
 * {@code scanBasePackages}. Spring-Cloud-Stream auto-configures {@code StreamBridge}
 * on its own so we do not have to mock it.
 */
@SpringBootTest(
        classes = CollectdApplication.class,
        properties = {
                "deltav.timeseries.enabled=true",
                "deltav.collectd.persister.inner.fail-fast=true",
                "deltav.collectd.persister.kafka.fail-fast=true",
                "spring.main.web-application-type=none",
                "spring.main.banner-mode=off",
                // opennms.home wired dynamically to overlays/collectd below
                "spring.autoconfigure.exclude=" +
                        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration," +
                        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration," +
                        "org.springframework.boot.autoconfigure.quartz.QuartzAutoConfiguration," +
                        "org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration," +
                        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration," +
                        "org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration",
                // Unresolvable brokers: all Kafka beans are mocked via @MockitoBean below
                "spring.kafka.bootstrap-servers=localhost:1",
                "spring.cloud.stream.kafka.binder.brokers=localhost:1",
                "opennms.kafka.bootstrap-servers=localhost:1",
                // Disable the Kafka RPC client — it connects to Kafka at startup and
                // needs DistPollerDao which triggers JPA/SQL against the mock DataSource.
                "opennms.rpc.kafka.enabled=false",
                "spring.main.allow-bean-definition-overriding=true"
        }
)
class CollectdApplicationScanIT {

    /**
     * Point opennms.home at the production overlays/collectd so the
     * many filesystem reads in {@link CollectdDaemonConfiguration} (the
     * {@code collectd-configuration.xml}, {@code snmp-config.xml},
     * {@code datacollection-config.xml}, {@code poll-outages.xml} loads plus
     * the {@code etc/datacollection/} directory scan) find real fixtures
     * instead of an empty {@code /tmp} directory. The overlay is committed
     * to the repo at a stable relative path so the IT is reproducible on CI.
     */
    @DynamicPropertySource
    static void overlayPath(DynamicPropertyRegistry reg) {
        // Module cwd is core/daemon-boot-collectd; overlay is at
        // deploy/overlays/collectd relative to repo root.
        Path overlay = Paths.get("..", "..", "deploy", "overlays",
                "collectd").toAbsolutePath().normalize();
        reg.add("opennms.home", () -> overlay.toString());
    }

    // ---- JPA / DAO layer ----
    @MockitoBean DataSource dataSource;
    @MockitoBean PlatformTransactionManager platformTransactionManager;
    @MockitoBean SessionUtils sessionUtils;
    @MockitoBean NodeDao nodeDao;
    @MockitoBean IpInterfaceDao ipInterfaceDao;

    // ---- Collectd config beans backed by real overlay fixtures above ----
    // filterDao can't come from overlay since it needs a real DataSource.
    @MockitoBean(name = "filterDao") FilterDao filterDao;

    // kafkaRpcMetricRegistry is normally provided by KafkaRpcClientConfiguration
    // (daemon-common) which we disable via opennms.rpc.kafka.enabled=false.
    // TimeseriesPersisterFactory depends on it, so supply a stand-in here.
    @MockitoBean(name = "kafkaRpcMetricRegistry") MetricRegistry kafkaRpcMetricRegistry;

    // The Collectd daemon bean exercises async collector futures on the
    // mocked LocationAwareCollectorClient at init; mock it out since we are
    // only asserting the timeseries wiring chain loads, not that collectd
    // actually polls anything.
    @MockitoBean Collectd collectd;

    // ---- Collection agent / RPC client chain ----
    @MockitoBean ServiceCollectorRegistry serviceCollectorRegistry;
    // Explicit bean names below: the M2 decorator registers a @Primary
    // AgentIdentityCapturingCollectorClient bean, so without explicit names,
    // @MockitoBean's type-based matching replaces the @Primary decorator and
    // leaves horizon's real bean to instantiate (failing on @Autowired
    // RpcTargetHelper since opennms.rpc.kafka.enabled=false in this test).
    @MockitoBean(name = "locationAwareCollectorClient") LocationAwareCollectorClient locationAwareCollectorClient;
    @MockitoBean(name = "agentIdentityCapturingCollectorClient") LocationAwareCollectorClient agentIdentityCapturingCollectorClient;
    @MockitoBean RpcClientFactory rpcClientFactory;
    @MockitoBean EntityScopeProvider entityScopeProvider;

    // ---- Event transport: avoid real Kafka producer at startup ----
    @MockitoBean(name = "kafkaEventForwarder") KafkaEventForwarder kafkaEventForwarder;
    @MockitoBean(name = "kafkaEventSubscriptionService")
    KafkaEventSubscriptionService kafkaEventSubscriptionService;

    @Autowired ApplicationContext ctx;

    @Test
    void persister_factory_autowires_to_FanoutPersisterFactory_when_flag_true() {
        PersisterFactory factory = ctx.getBean(
                "compositePersisterFactory", PersisterFactory.class);
        // FanoutPersisterFactory is a package-private static nested class of
        // TimeseriesKafkaPublisherConfiguration, so we check by qualified name
        // rather than importing the type (which won't compile from this
        // package).
        assertThat(factory.getClass().getName())
                .as("with deltav.timeseries.enabled=true, the @Primary "
                        + "compositePersisterFactory must be FanoutPersisterFactory")
                .isEqualTo("org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration$FanoutPersisterFactory");
    }

    @Test
    void deltav_timeseries_topic_bean_exists_when_flag_true() {
        NewTopic topic = ctx.getBean("deltavTimeseriesTopic", NewTopic.class);
        assertThat(topic).isNotNull();
        assertThat(topic.name()).isEqualTo("deltav-timeseries");
    }

    @Test
    void timeseries_kafka_publisher_bean_exists_when_flag_true() {
        TimeseriesKafkaPublisher publisher = ctx.getBean(TimeseriesKafkaPublisher.class);
        assertThat(publisher).isNotNull();
    }
}
