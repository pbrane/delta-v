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



import io.micrometer.core.instrument.MeterRegistry;

import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.opennms.netmgt.dao.api.AlarmEntityNotifier;
import org.opennms.netmgt.alarmd.Alarmd;
import org.opennms.netmgt.alarmd.AlarmLifecycleListenerManager;
import org.opennms.netmgt.alarmd.AlarmPersister;
import org.opennms.netmgt.alarmd.AlarmPersisterImpl;
import org.opennms.netmgt.alarmd.NorthbounderManager;
import org.opennms.netmgt.events.api.AnnotationBasedEventListenerAdapter;
import org.opennms.netmgt.events.api.EventSubscriptionService;
import org.opennms.netmgt.model.AlarmAssociation;
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
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.transaction.support.TransactionOperations;

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
     * No-op EventProxy — Alarmd's NorthbounderManager uses this to send events
     * for northbound interface state changes. Not needed until NBI integration.
     */
    @Bean(name = "eventProxy")
    public org.opennms.netmgt.events.api.EventProxy eventProxy() {
        return new org.opennms.netmgt.events.api.EventProxy() {
            @Override public void send(org.opennms.netmgt.xml.event.Event event) {}
            @Override public void send(org.opennms.netmgt.xml.event.Log eventLog) {}
        };
    }

    @Bean
    public NorthbounderManager northbounderManager() {
        return new NorthbounderManager();
    }

    @Bean
    public Alarmd alarmd(AlarmPersister alarmPersister,
                         AlarmLifecycleListenerManager alarmLifecycleListenerManager,
                         NorthbounderManager northbounderManager) {
        return new Alarmd(alarmPersister, alarmLifecycleListenerManager, northbounderManager,
                null, null);
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
