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
package org.deltav.netmgt.perspectivepoller.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
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
import org.opennms.netmgt.dao.api.ApplicationDao;
import org.opennms.netmgt.dao.api.ServicePerspective;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.opennms.netmgt.poller.ServiceMonitor;
import org.opennms.netmgt.poller.ServiceMonitorRegistry;

/**
 * Pins the FR9 classification for PerspectivePollerd: a perspective-polled service type lands on the
 * {@code deltav_perspectivepollerd_services_unscheduled} gauge when it has no catalog definition OR its
 * monitor class is missing from the registry, and is excluded when its definition is {@code enabled: false}
 * (disabled by choice) or fully schedulable.
 *
 * <p>The behaviour mirrors pollerd's {@code CatalogStartupCheck}, but over a different universe: only the
 * services that are members of an application mapped to a perspective location
 * ({@code ApplicationDao.getServicePerspectives()}) are evaluated — never all of inventory.</p>
 */
class PerspectiveCatalogStartupCheckTest {

    private static final String ICMP_MONITOR = "org.opennms.netmgt.poller.monitors.IcmpMonitor";
    private static final String MISSING_MONITOR = "org.opennms.netmgt.poller.monitors.MissingMonitor";

    @Test
    void gaugeCarriesOnlyGenuineGaps() {
        final Catalog catalog = new Catalog(List.of(
                def("ICMP", ICMP_MONITOR, true),        // schedulable -> not on gauge
                def("HTTP", MISSING_MONITOR, true),     // monitor absent -> gap
                def("Legacy", MISSING_MONITOR, false))); // disabled by choice -> excluded DESPITE missing monitor
        // Legacy uses a missing monitor on purpose: the disabled flag is the SOLE reason it stays off the
        // gauge, so a broken enabled-check would flip it to a (missing-monitor) gap and fail this test.
        // The perspective universe also contains "Mystery" with no definition at all -> gap.
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        final PerspectiveCatalogStartupCheck check = new PerspectiveCatalogStartupCheck(
                catalog,
                registryWith(ICMP_MONITOR),
                daoWithPerspectiveTypes("ICMP", "HTTP", "Legacy", "Mystery"),
                directSessionUtils(),
                registry);

        check.recompute(true);

        assertEquals(Set.of("HTTP", "Mystery"), unscheduledServices(registry),
                "only no-definition and missing-monitor perspective types belong on the gauge");
    }

    @Test
    void noGapsLeavesGaugeEmpty() {
        final Catalog catalog = new Catalog(List.of(def("ICMP", ICMP_MONITOR, true)));
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new PerspectiveCatalogStartupCheck(catalog, registryWith(ICMP_MONITOR),
                daoWithPerspectiveTypes("ICMP"), directSessionUtils(), registry).recompute(true);

        assertEquals(Set.of(), unscheduledServices(registry));
    }

    @Test
    void gaugeClearsStaleSeriesWhenGapResolved() {
        // HTTP's monitor is initially absent from the registry -> reported as a gap.
        final Set<String> registered = new HashSet<>(Set.of(ICMP_MONITOR));
        final Catalog catalog = new Catalog(List.of(def("HTTP", MISSING_MONITOR, true)));
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        final PerspectiveCatalogStartupCheck check = new PerspectiveCatalogStartupCheck(
                catalog, registry(registered), daoWithPerspectiveTypes("HTTP"), directSessionUtils(), registry);

        check.recompute(true);
        assertEquals(Set.of("HTTP"), unscheduledServices(registry), "gap should be reported first");

        // The monitor becomes available -> the next recompute must drop the now-stale series.
        registered.add(MISSING_MONITOR);
        check.recompute(false);
        assertEquals(Set.of(), unscheduledServices(registry),
                "a resolved gap must be removed from the gauge (No data = healthy)");
    }

    @Test
    void duplicatePerspectiveRowsCollapseToOneSeries() {
        // getServicePerspectives() returns one row per (service x perspective-location), so the same service
        // legitimately recurs — and resolution is case-insensitive. A single undefined service spread across
        // several rows (and case variants) must collapse to exactly ONE gauge series, not one per row. This
        // pins the case-insensitive TreeSet dedup; a HashSet/case-sensitive regression would blow up gauge
        // cardinality and still pass every other test. This is the axis where perspectivepollerd diverges
        // from pollerd (whose inventory query yields one row per type).
        final Catalog catalog = new Catalog(List.of(def("ICMP", ICMP_MONITOR, true)));
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new PerspectiveCatalogStartupCheck(catalog, registryWith(ICMP_MONITOR),
                daoWithPerspectiveTypes("HTTP", "http", "HTTP"), directSessionUtils(), registry).recompute(true);

        assertEquals(1, unscheduledServices(registry).size(),
                "duplicate / mixed-case perspective rows for one undefined service must collapse to a single series");
    }

    @Test
    void blankAndNullServiceNamesAreIgnored() {
        // A perspective row with a blank or null service name must be filtered out, not flagged as a gap.
        final Catalog catalog = new Catalog(List.of(def("ICMP", ICMP_MONITOR, true)));
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new PerspectiveCatalogStartupCheck(catalog, registryWith(ICMP_MONITOR),
                daoWithPerspectiveTypes("ICMP", "", null), directSessionUtils(), registry).recompute(true);

        assertEquals(Set.of(), unscheduledServices(registry),
                "blank/null perspective service names must be ignored, not reported as unresolved");
    }

    // --- helpers ---

    private static Set<String> unscheduledServices(final SimpleMeterRegistry registry) {
        return registry.getMeters().stream()
                .filter(m -> m instanceof Gauge)
                .filter(m -> PerspectivePollerdDomainMetrics.SERVICES_UNSCHEDULED.equals(m.getId().getName()))
                .map(m -> m.getId().getTag(PerspectivePollerdDomainMetrics.TAG_SERVICE))
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

    /**
     * An {@link ApplicationDao} whose {@code getServicePerspectives()} returns one perspective row per given
     * service name (perspective location is irrelevant to the check, so it is left null).
     */
    private static ApplicationDao daoWithPerspectiveTypes(final String... serviceNames) {
        // ServicePerspective requireNonNull-s both args; the check only reads getService().getServiceName(),
        // so the perspective location is an inert non-null mock.
        final OnmsMonitoringLocation location = mock(OnmsMonitoringLocation.class);
        final List<ServicePerspective> perspectives = new ArrayList<>(serviceNames.length);
        for (final String name : serviceNames) {
            final OnmsMonitoredService svc = mock(OnmsMonitoredService.class);
            when(svc.getServiceName()).thenReturn(name);
            perspectives.add(new ServicePerspective(svc, location));
        }
        final ApplicationDao dao = mock(ApplicationDao.class);
        when(dao.getServicePerspectives()).thenReturn(perspectives);
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
