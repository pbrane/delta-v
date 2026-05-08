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
package org.deltav.netmgt.alarmd.boot;



import java.io.File;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManagerFactory;

import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.hibernate.SessionFactory;
import org.opennms.netmgt.dao.api.AlarmEntityNotifier;
import org.opennms.netmgt.alarmd.Alarmd;
import org.opennms.netmgt.alarmd.AlarmLifecycleListenerManager;
import org.opennms.netmgt.alarmd.AlarmPersister;
import org.opennms.netmgt.alarmd.AlarmPersisterImpl;
import org.opennms.netmgt.alarmd.NorthbounderManager;
import org.opennms.netmgt.alarmd.drools.AlarmService;
import org.opennms.netmgt.alarmd.drools.AlarmTicketerService;
import org.opennms.netmgt.alarmd.drools.DefaultAlarmService;
import org.opennms.netmgt.alarmd.drools.DefaultAlarmTicketerService;
import org.opennms.netmgt.alarmd.drools.DroolsAlarmContext;
import org.opennms.netmgt.events.api.AnnotationBasedEventListenerAdapter;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.events.api.EventProxy;
import org.opennms.netmgt.events.api.EventSubscriptionService;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Log;
import org.opennms.netmgt.model.AlarmAssociation;
import org.opennms.netmgt.model.OnmsAcknowledgment;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsCategory;
import org.opennms.netmgt.model.OnmsDistPoller;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsMemo;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.OnmsMonitoringSystem;
import org.opennms.netmgt.model.OnmsAssetRecord;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsReductionKeyMemo;
import org.opennms.netmgt.model.OnmsServiceType;
import org.opennms.netmgt.model.OnmsApplication;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.opennms.netmgt.model.jakarta.converter.InetAddressConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeLabelSourceConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeTypeConverter;
import org.opennms.netmgt.model.jakarta.converter.OnmsSeverityConverter;
import org.opennms.netmgt.model.jakarta.converter.PrimaryTypeConverter;
import org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring Boot @Configuration that wires all Alarmd beans.
 *
 * <p>This replaces the Karaf-era {@code applicationContext-daemon-loader-alarmd.xml}.
 * Beans that depend on DAOs and other infrastructure services (EventUtil,
 * AlarmEntityNotifier, SessionUtils, EventProxy, etc.) receive those dependencies
 * via @Autowired field injection in the existing classes. The actual DAO beans
 * are expected to be provided by a separate configuration (e.g., JPA auto-config
 * or a dedicated DAO configuration class).</p>
 *
 * <p>The {@link AnnotationBasedEventListenerAdapter} bridges Alarmd's
 * {@code @EventHandler}-annotated methods to the {@link EventSubscriptionService},
 * registering Alarmd as an event listener during {@code afterPropertiesSet()}.</p>
 *
 * <p>Entity classes are listed explicitly via a custom {@link PersistenceManagedTypes}
 * bean instead of using package-based {@code @EntityScan} because the legacy
 * opennms-model module shares the same package ({@code org.opennms.netmgt.model})
 * and contains classes with incompatible javax.persistence / Hibernate 3.x
 * annotations that cause scanning failures with Hibernate 7.</p>
 */
@Configuration
public class AlarmdConfiguration {

    /**
     * Use standard JPA naming — table/column names from @Table/@Column annotations
     * are used as-is, without Spring Boot's default CamelCase→snake_case conversion.
     * This is required because the OpenNMS schema uses camelCase table names
     * (e.g., monitoringSystems, ipInterface, ifServices).
     */
    @Bean
    public PhysicalNamingStrategyStandardImpl physicalNamingStrategy() {
        return new PhysicalNamingStrategyStandardImpl();
    }

    /**
     * Explicitly lists the Jakarta entity classes to register with Hibernate 7.
     * This replaces {@code @EntityScan(basePackages = "org.opennms.netmgt.model")}
     * which would scan ALL classes in the package, including legacy entities
     * with incompatible javax.persistence annotations.
     */
    @Bean
    public PersistenceManagedTypes persistenceManagedTypes() {
        return PersistenceManagedTypes.of(
            OnmsAlarm.class.getName(),
            OnmsAcknowledgment.class.getName(),
            AlarmAssociation.class.getName(),
            OnmsCategory.class.getName(),
            OnmsDistPoller.class.getName(),
            OnmsIpInterface.class.getName(),
            OnmsMemo.class.getName(),
            OnmsMonitoredService.class.getName(),
            OnmsMonitoringSystem.class.getName(),
            OnmsNode.class.getName(),
            OnmsAssetRecord.class.getName(),
            OnmsReductionKeyMemo.class.getName(),
            OnmsServiceType.class.getName(),
            OnmsSnmpInterface.class.getName(),
            OnmsMonitoringLocation.class.getName(),
            OnmsApplication.class.getName(),
            // AttributeConverters (autoApply=true)
            NodeTypeConverter.class.getName(),
            PrimaryTypeConverter.class.getName(),
            InetAddressConverter.class.getName(),
            NodeLabelSourceConverter.class.getName(),
            OnmsSeverityConverter.class.getName()
        );
    }

    /**
     * SessionUtils implementation backed by Spring's TransactionTemplate.
     * Replaces the OSGi-era SessionUtils that was used to bridge Hibernate
     * sessions across OSGi bundles.
     */
    @Bean
    public org.opennms.netmgt.dao.api.SessionUtils sessionUtils(
            org.springframework.transaction.PlatformTransactionManager txManager) {
        var txTemplate = new org.springframework.transaction.support.TransactionTemplate(txManager);
        var readOnlyTxTemplate = new org.springframework.transaction.support.TransactionTemplate(txManager);
        readOnlyTxTemplate.setReadOnly(true);
        return new org.opennms.netmgt.dao.api.SessionUtils() {
            @Override
            public <V> V withTransaction(java.util.function.Supplier<V> supplier) {
                return txTemplate.execute(status -> supplier.get());
            }
            @Override
            public <V> V withReadOnlyTransaction(java.util.function.Supplier<V> supplier) {
                return readOnlyTxTemplate.execute(status -> supplier.get());
            }
            @Override
            public <V> V withManualFlush(java.util.function.Supplier<V> supplier) {
                return supplier.get();
            }
        };
    }

    /**
     * Provides TransactionOperations for AlarmPersisterImpl.
     * Spring Boot auto-creates a PlatformTransactionManager; we wrap it
     * in a TransactionTemplate for the @Autowired TransactionOperations field.
     */
    @Bean
    public TransactionOperations transactionOperations(
            org.springframework.transaction.PlatformTransactionManager txManager) {
        return new org.springframework.transaction.support.TransactionTemplate(txManager);
    }

    /**
     * Counting AlarmEntityNotifier — increments {@code deltav_alarmd_alarms_*}
     * Micrometer counters on every lifecycle notification. Downstream listener
     * integration (BSMd, REST callers) is tracked in memory
     * {@code project_alarmd_alarm_lifecycle_gap}; this bean only surfaces
     * domain signal, it does not forward events.
     */
    @Bean
    public AlarmEntityNotifier alarmEntityNotifier(MeterRegistry meterRegistry) {
        return new CountingAlarmEntityNotifier(meterRegistry);
    }

    @Bean
    public AlarmPersisterImpl alarmPersister() {
        return new AlarmPersisterImpl();
    }

    @Bean
    public AlarmLifecycleListenerManager alarmLifecycleListenerManager() {
        return new AlarmLifecycleListenerManager();
    }

    /**
     * Bridges horizon's older {@link EventProxy} interface to delta-v's
     * {@link EventForwarder} (KafkaEventForwarder) bean. Used by
     * {@link NorthbounderManager} to send NBI state-change events, and by
     * any horizon code path that resolves the {@code eventProxy} bean by
     * name. Per {@code feedback_horizon_parallel_vs_additive}, EventProxy
     * and EventForwarder are parallel abstractions — until horizon source
     * deletes EventProxy entirely, an in-process delegate is the right fix.
     */
    @Bean(name = "eventProxy")
    public EventProxy eventProxy(EventForwarder eventForwarder) {
        return new EventProxy() {
            @Override public void send(Event event) { eventForwarder.sendNow(event); }
            @Override public void send(Log eventLog) { eventForwarder.sendNow(eventLog); }
        };
    }

    @Bean
    public NorthbounderManager northbounderManager() {
        return new NorthbounderManager();
    }

    /**
     * Exposes the Hibernate {@link SessionFactory} as a bean for
     * {@link DroolsAlarmContext}'s {@code @Autowired SessionFactory} field.
     * Spring Boot auto-configures an {@link EntityManagerFactory} via
     * {@code spring-boot-starter-data-jpa}; Hibernate's SessionFactory is
     * the underlying instance reachable via {@code unwrap}.
     */
    @Bean
    public SessionFactory sessionFactory(EntityManagerFactory emf) {
        return emf.unwrap(SessionFactory.class);
    }

    /**
     * {@link TransactionTemplate} bean — DroolsAlarmContext autowires this
     * concrete type (not the {@link TransactionOperations} interface).
     * Sharing the same instance across both bean definitions is fine
     * because TransactionTemplate is thread-safe.
     */
    @Bean
    public TransactionTemplate transactionTemplate(
            org.springframework.transaction.PlatformTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }

    /**
     * Drools facade for the Right-Hand-Side actions in alarmd / situations
     * rules: clear, ack, escalate, set-severity, etc. Backs DroolsAlarmContext.
     */
    @Bean
    public AlarmService alarmService() {
        return new DefaultAlarmService();
    }

    /**
     * Drools facade for ticketer integration (createTicket / updateTicket
     * / closeTicket events). Publishes troubleTicket/* UEIs through the
     * existing {@link EventForwarder}; the actual ticket-system dispatch
     * happens in a future Kafka consumer per
     * {@code project_external_alarm_integrations_as_kafka_consumers}.
     * Until that consumer ships, ticket events flow to Kafka but are not
     * consumed; that is harmless (no rules require ticket round-trip).
     */
    @Bean
    public AlarmTicketerService alarmTicketerService() {
        return new DefaultAlarmTicketerService();
    }

    /**
     * Drools KieSession context that runs alarmd.drl + situations.drl on
     * every alarm-lifecycle callback (created / cleared / acked / etc.).
     * Without this bean, alarmd is "store and forget": alarms persist but
     * never auto-clear, escalate, or archive.
     *
     * <p>The rules folder is baked into the alarmd container via
     * {@code alarmd-overlay/etc/alarmd/drools-rules.d/} (bundled here from
     * horizon's {@code opennms-base-assembly/.../drools-rules.d}). Default
     * resolution via {@code DroolsAlarmContext#getDefaultRulesFolder()}
     * relies on {@code ConfigFileConstants.getHome()} which reads the
     * {@code opennms.home} system property; we pass the path explicitly to
     * avoid that brittle indirection.</p>
     */
    @Bean
    public DroolsAlarmContext droolsAlarmContext(
            @Value("${opennms.home:/opt/deltav}") String opennmsHome) {
        File rulesFolder = new File(opennmsHome, "etc/alarmd/drools-rules.d");
        return new DroolsAlarmContext(rulesFolder);
    }

    /**
     * <p>The Alarmd daemon — wired with DroolsAlarmContext for full alarm
     * lifecycle automation. The 5th constructor argument ({@code MessageBus})
     * stays {@code null}: in delta-v's containerized deployment model,
     * horizon's reload-config IPC has no analog (config reload = container
     * recreate), and horizon's own {@code Alarmd.onInit()} tolerates a null
     * MessageBus by logging a single warning. The next horizon dep bump
     * will drop the parameter entirely (see
     * {@code project_horizon_alarmd_messagebus_removal}).</p>
     */
    @Bean
    public Alarmd alarmd(AlarmPersister alarmPersister,
                         AlarmLifecycleListenerManager alarmLifecycleListenerManager,
                         NorthbounderManager northbounderManager,
                         DroolsAlarmContext droolsAlarmContext) {
        return new Alarmd(alarmPersister, alarmLifecycleListenerManager, northbounderManager,
                droolsAlarmContext, null /* MessageBus — see javadoc */);
    }

    @Bean
    public AnnotationBasedEventListenerAdapter alarmdEventListenerAdapter(
            Alarmd alarmd,
            @org.springframework.beans.factory.annotation.Qualifier("kafkaEventSubscriptionService")
            EventSubscriptionService eventSubscriptionService) {
        var adapter = new AnnotationBasedEventListenerAdapter();
        adapter.setAnnotatedListener(alarmd);
        adapter.setEventSubscriptionService(eventSubscriptionService);
        return adapter;
    }

    @Bean
    public SmartLifecycle alarmdLifecycle(Alarmd alarmd) {
        return new SpringServiceDaemonSmartLifecycle(alarmd);
    }
}
