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
package org.deltav.netmgt.poller.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.poller.catalog.Catalog;
import org.deltav.poller.catalog.ServiceDefinition;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.poller.ServiceMonitor;
import org.opennms.netmgt.poller.ServiceMonitorRegistry;

/**
 * Pins the FR9 classification: an inventory service type lands on the
 * {@code deltav_pollerd_services_unscheduled} gauge when it has no catalog definition OR its
 * monitor class is missing from the registry, and is excluded when its definition is
 * {@code enabled: false} (disabled by choice) or fully schedulable.
 */
class CatalogStartupCheckTest {

    private static final String ICMP_MONITOR = "org.opennms.netmgt.poller.monitors.IcmpMonitor";
    private static final String MISSING_MONITOR = "org.opennms.netmgt.poller.monitors.MissingMonitor";

    @Test
    void gaugeCarriesOnlyGenuineGaps() {
        final Catalog catalog = new Catalog(List.of(
                def("ICMP", ICMP_MONITOR, true),        // schedulable -> not on gauge
                def("HTTP", MISSING_MONITOR, true),     // monitor absent -> gap
                def("Legacy", ICMP_MONITOR, false)));   // disabled by choice -> excluded
        // Inventory also contains "Mystery" with no definition at all -> gap.
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        final CatalogStartupCheck check = new CatalogStartupCheck(
                catalog,
                registryWith(ICMP_MONITOR),
                daoWithTypes("ICMP", "HTTP", "Legacy", "Mystery"),
                directSessionUtils(),
                registry);

        check.recompute(true);

        assertEquals(Set.of("HTTP", "Mystery"), unscheduledServices(registry),
                "only no-definition and missing-monitor types belong on the gauge");
    }

    @Test
    void noGapsLeavesGaugeEmpty() {
        final Catalog catalog = new Catalog(List.of(def("ICMP", ICMP_MONITOR, true)));
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new CatalogStartupCheck(catalog, registryWith(ICMP_MONITOR), daoWithTypes("ICMP"),
                directSessionUtils(), registry).recompute(true);

        assertEquals(Set.of(), unscheduledServices(registry));
    }

    @Test
    void gaugeClearsStaleSeriesWhenGapResolved() {
        // HTTP's monitor is initially absent from the registry -> reported as a gap.
        final Set<String> registered = new HashSet<>(Set.of(ICMP_MONITOR));
        final Catalog catalog = new Catalog(List.of(def("HTTP", MISSING_MONITOR, true)));
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        final CatalogStartupCheck check = new CatalogStartupCheck(
                catalog, registry(registered), daoWithTypes("HTTP"), directSessionUtils(), registry);

        check.recompute(true);
        assertEquals(Set.of("HTTP"), unscheduledServices(registry), "gap should be reported first");

        // The monitor becomes available -> the next recompute must drop the now-stale series.
        registered.add(MISSING_MONITOR);
        check.recompute(false);
        assertEquals(Set.of(), unscheduledServices(registry),
                "a resolved gap must be removed from the gauge (No data = healthy)");
    }

    // --- helpers ---

    private static Set<String> unscheduledServices(final SimpleMeterRegistry registry) {
        return registry.getMeters().stream()
                .filter(m -> m instanceof Gauge)
                .filter(m -> PollerdDomainMetrics.SERVICES_UNSCHEDULED.equals(m.getId().getName()))
                .map(m -> m.getId().getTag(PollerdDomainMetrics.TAG_SERVICE))
                .collect(Collectors.toSet());
    }

    private static ServiceDefinition def(final String name, final String monitor, final boolean enabled) {
        return new ServiceDefinition(name, null, monitor, 30000, enabled, Map.of());
    }

    private static ServiceMonitorRegistry registryWith(final String... classNames) {
        return registry(new HashSet<>(Set.of(classNames)));
    }

    /** Reads {@code known} live, so a test can mutate the set to simulate a monitor being (un)registered. */
    private static ServiceMonitorRegistry registry(final Set<String> known) {
        final ServiceMonitor monitor = mock(ServiceMonitor.class);
        return new ServiceMonitorRegistry() {
            @Override
            public ServiceMonitor getMonitorByClassName(final String className) {
                return known.contains(className) ? monitor : null;
            }

            @Override
            public Set<String> getMonitorClassNames() {
                return known;
            }
        };
    }

    private static MonitoredServiceDao daoWithTypes(final String... serviceNames) {
        final MonitoredServiceDao dao = mock(MonitoredServiceDao.class);
        final List<OnmsMonitoredService> services = List.of(serviceNames).stream()
                .map(name -> {
                    final OnmsMonitoredService svc = mock(OnmsMonitoredService.class);
                    when(svc.getServiceName()).thenReturn(name);
                    return svc;
                })
                .collect(Collectors.toList());
        when(dao.findAllServicesForScheduling()).thenReturn(services);
        return dao;
    }

    /** SessionUtils that runs the supplier inline — no transaction needed in a unit test. */
    private static SessionUtils directSessionUtils() {
        return new SessionUtils() {
            @Override public <V> V withTransaction(final Supplier<V> supplier) { return supplier.get(); }
            @Override public <V> V withReadOnlyTransaction(final Supplier<V> supplier) { return supplier.get(); }
            @Override public <V> V withManualFlush(final Supplier<V> supplier) { return supplier.get(); }
        };
    }
}
