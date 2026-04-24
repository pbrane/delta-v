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
package org.deltav.netmgt.bsm.boot;

import java.util.HashMap;

import org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl;
import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.opennms.netmgt.alarmd.AlarmLifecycleListenerManager;
import org.opennms.netmgt.bsm.daemon.Bsmd;
import org.opennms.netmgt.bsm.persistence.api.ApplicationEdgeEntity;
import org.opennms.netmgt.bsm.persistence.api.BusinessServiceChildEdgeEntity;
import org.opennms.netmgt.bsm.persistence.api.BusinessServiceEdgeEntity;
import org.opennms.netmgt.bsm.persistence.api.BusinessServiceEntity;
import org.opennms.netmgt.bsm.persistence.api.IPServiceEdgeEntity;
import org.opennms.netmgt.bsm.persistence.api.SingleReductionKeyEdgeEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.map.AbstractMapFunctionEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.map.DecreaseEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.map.IdentityEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.map.IgnoreEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.map.IncreaseEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.map.SetToEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.reduce.AbstractReductionFunctionEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.reduce.ExponentialPropagationEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.reduce.HighestSeverityAboveEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.reduce.HighestSeverityEntity;
import org.opennms.netmgt.bsm.persistence.api.functions.reduce.ThresholdEntity;
import org.opennms.netmgt.bsm.service.BusinessServiceManager;
import org.opennms.netmgt.bsm.service.BusinessServiceStateMachine;
import org.opennms.netmgt.bsm.service.internal.BusinessServiceManagerImpl;
import org.opennms.netmgt.bsm.service.internal.DefaultBusinessServiceStateMachine;
import org.opennms.netmgt.bsm.persistence.api.BusinessServiceDao;
import org.opennms.netmgt.bsm.persistence.api.BusinessServiceEdgeDao;
import org.opennms.netmgt.bsm.persistence.api.functions.map.MapFunctionDao;
import org.opennms.netmgt.bsm.persistence.api.functions.reduce.ReductionFunctionDao;
import org.deltav.core.daemon.common.DaemonEventConfDao;
import org.opennms.core.messagebus.MessageBus;
import org.opennms.netmgt.config.api.EventConfDao;
import org.opennms.netmgt.dao.api.ApplicationDao;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.events.api.AnnotationBasedEventListenerAdapter;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.events.api.EventSubscriptionService;
import org.opennms.netmgt.model.AlarmAssociation;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsApplication;
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
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.opennms.netmgt.model.jakarta.converter.InetAddressConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeLabelSourceConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeTypeConverter;
import org.opennms.netmgt.model.jakarta.converter.OnmsSeverityConverter;
import org.opennms.netmgt.model.jakarta.converter.PrimaryTypeConverter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring Boot @Configuration that wires all BSMd beans.
 *
 * <p>This replaces the Karaf-era BSMd blueprint XML and
 * {@code applicationContext-daemon-loader-bsmd.xml}.
 * Beans that depend on DAOs and other infrastructure services receive those
 * dependencies via constructor injection or @Autowired field injection in the
 * existing classes. The actual DAO beans are provided by JPA auto-config
 * and the {@code @Repository}-annotated DAO classes scanned from
 * {@code org.opennms.netmgt.bsm.dao}.</p>
 *
 * <p>Entity classes are listed explicitly via a custom {@link PersistenceManagedTypes}
 * bean instead of using package-based {@code @EntityScan} because the legacy
 * opennms-model module shares the same package ({@code org.opennms.netmgt.model})
 * and contains classes with incompatible javax.persistence / Hibernate 3.x
 * annotations that cause scanning failures with Hibernate 7.</p>
 */
@Configuration
public class BsmdConfiguration {

    /**
     * Use standard JPA naming -- table/column names from @Table/@Column annotations
     * are used as-is, without Spring Boot's default CamelCase-to-snake_case conversion.
     * This is required because the OpenNMS schema uses camelCase table names
     * (e.g., monitoringSystems, ipInterface, ifServices).
     */
    @Bean
    public PhysicalNamingStrategyStandardImpl physicalNamingStrategy() {
        return new PhysicalNamingStrategyStandardImpl();
    }

    /**
     * Explicitly lists the Jakarta entity classes to register with Hibernate 7.
     * This replaces {@code @EntityScan} which would scan ALL classes in the
     * package, including legacy entities with incompatible javax.persistence
     * annotations.
     *
     * <p>Includes both core model entities (OnmsAlarm, OnmsNode, etc.) and
     * BSM-specific entities (BusinessServiceEntity, edge entities, reduction
     * function entities, map function entities).</p>
     */
    @Bean
    public PersistenceManagedTypes persistenceManagedTypes() {
        return PersistenceManagedTypes.of(
            // Core model entities
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
            // BSM entities
            BusinessServiceEntity.class.getName(),
            BusinessServiceEdgeEntity.class.getName(),
            BusinessServiceChildEdgeEntity.class.getName(),
            IPServiceEdgeEntity.class.getName(),
            ApplicationEdgeEntity.class.getName(),
            SingleReductionKeyEdgeEntity.class.getName(),
            // Reduction function entities
            AbstractReductionFunctionEntity.class.getName(),
            HighestSeverityEntity.class.getName(),
            HighestSeverityAboveEntity.class.getName(),
            ThresholdEntity.class.getName(),
            ExponentialPropagationEntity.class.getName(),
            // Map function entities
            AbstractMapFunctionEntity.class.getName(),
            IdentityEntity.class.getName(),
            IgnoreEntity.class.getName(),
            DecreaseEntity.class.getName(),
            IncreaseEntity.class.getName(),
            SetToEntity.class.getName(),
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
    public org.opennms.netmgt.dao.api.SessionUtils sessionUtils(PlatformTransactionManager txManager) {
        var txTemplate = new TransactionTemplate(txManager);
        var readOnlyTxTemplate = new TransactionTemplate(txManager);
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
     * TransactionTemplate for Bsmd constructor injection.
     * Also serves as TransactionOperations (TransactionTemplate implements it).
     */
    @Bean
    public TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }

    /**
     * AlarmProvider that looks up alarms by reduction key using JPA/HQL.
     *
     * <p>Replaces the default {@code AlarmProviderImpl} which uses the legacy
     * {@code findMatching(Criteria)} API that is not implemented in the
     * Jakarta/Hibernate 7 DAO layer.</p>
     */
    @Bean
    public org.opennms.netmgt.bsm.service.AlarmProvider alarmProvider(
            org.opennms.netmgt.dao.api.AlarmDao alarmDao,
            MeterRegistry meterRegistry) {
        Timer lookupTimer = meterRegistry.timer(BsmdDomainMetrics.ALARM_LOOKUP_DURATION);
        return reductionKeys -> lookupTimer.record(() -> {
            meterRegistry.counter(BsmdDomainMetrics.ALARM_LOOKUPS).increment();
            if (reductionKeys == null || reductionKeys.isEmpty()) {
                return new HashMap<>();
            }
            meterRegistry.counter(BsmdDomainMetrics.ALARM_LOOKUP_KEYS_REQUESTED)
                    .increment(reductionKeys.size());
            var matches = alarmDao.findAll().stream()
                    .filter(a -> reductionKeys.contains(a.getReductionKey()))
                    .collect(java.util.stream.Collectors.toMap(
                            OnmsAlarm::getReductionKey,
                            a -> (org.opennms.netmgt.bsm.service.model.AlarmWrapper)
                                    new org.opennms.netmgt.bsm.service.internal.AlarmWrapperImpl(a)));
            meterRegistry.counter(BsmdDomainMetrics.ALARM_LOOKUP_KEYS_MATCHED)
                    .increment(matches.size());
            return matches;
        });
    }


    @Bean
    public BusinessServiceStateMachine businessServiceStateMachine(
            org.opennms.netmgt.bsm.service.AlarmProvider alarmProvider) {
        return new DefaultBusinessServiceStateMachine(alarmProvider);
    }

    /**
     * The Business Service manager that provides CRUD operations for
     * Business Services and their edges.
     */
    @Bean
    public BusinessServiceManager businessServiceManager(
            BusinessServiceDao businessServiceDao,
            BusinessServiceEdgeDao edgeDao,
            MonitoredServiceDao monitoredServiceDao,
            MapFunctionDao mapFunctionDao,
            ReductionFunctionDao reductionFunctionDao,
            BusinessServiceStateMachine businessServiceStateMachine,
            NodeDao nodeDao,
            @Qualifier("eventIpcManager") EventForwarder eventForwarder,
            ApplicationDao applicationDao) {
        return new BusinessServiceManagerImpl(
                businessServiceDao,
                edgeDao,
                monitoredServiceDao,
                mapFunctionDao,
                reductionFunctionDao,
                businessServiceStateMachine,
                nodeDao,
                eventForwarder,
                applicationDao);
    }

    /**
     * Manages alarm lifecycle listeners. BSMd registers itself as a listener
     * to receive alarm state change notifications.
     */
    @Bean
    public AlarmLifecycleListenerManager alarmLifecycleListenerManager() {
        return new AlarmLifecycleListenerManager();
    }

    /**
     * Registers Bsmd as an alarm lifecycle listener after all beans are
     * fully constructed, avoiding circular dependency issues during
     * bean initialization. Then triggers an immediate alarm snapshot so
     * that BSMd picks up any alarms that were created before it started.
     *
     * <p>The AlarmLifecycleListenerManager's timer fires its first snapshot
     * at delay=0 (before this callback runs), so without the explicit
     * doSnapshot() call here, BSMd would have to wait for the next timer
     * tick (default 2 minutes) to get its first alarm state.</p>
     */
    @Bean
    public SmartInitializingSingleton registerBsmdAsAlarmListener(
            AlarmLifecycleListenerManager manager,
            Bsmd bsmd) {
        return () -> {
            manager.onListenerRegistered(bsmd, new HashMap<>());
            manager.doSnapshot();
        };
    }

    /**
     * Default event configuration DAO that reads eventconf.xml from
     * the OpenNMS configuration directory.
     */
    @Bean
    public EventConfDao eventConfDao() {
        return new DaemonEventConfDao();
    }

    /**
     * The BSM daemon that drives the Business Service state machine by
     * reacting to alarm lifecycle events and sending BS status change events.
     */
    @Bean
    public Bsmd bsmd(
            @Qualifier("eventIpcManager") EventIpcManager eventIpcManager,
            EventConfDao eventConfDao,
            TransactionTemplate transactionTemplate,
            BusinessServiceStateMachine stateMachine,
            BusinessServiceManager manager,
            ObjectProvider<MessageBus> messageBusProvider) {
        return new Bsmd(eventIpcManager, eventConfDao, transactionTemplate, stateMachine, manager,
                messageBusProvider.getIfAvailable());
    }

    /**
     * Bridges Bsmd's {@code @EventHandler}-annotated methods to the
     * {@link EventSubscriptionService}, registering Bsmd as an event listener.
     */
    @Bean
    public AnnotationBasedEventListenerAdapter bsmdEventListenerAdapter(
            Bsmd bsmd,
            @Qualifier("kafkaEventSubscriptionService") EventSubscriptionService eventSubscriptionService) {
        var adapter = new AnnotationBasedEventListenerAdapter();
        adapter.setAnnotatedListener(bsmd);
        adapter.setEventSubscriptionService(eventSubscriptionService);
        return adapter;
    }

    /**
     * SmartLifecycle adapter that starts/stops Bsmd at the appropriate
     * phase in the Spring application lifecycle.
     */
    @Bean
    public SmartLifecycle bsmdLifecycle(Bsmd bsmd) {
        return new SpringServiceDaemonSmartLifecycle(bsmd, Bsmd.NAME);
    }
}
