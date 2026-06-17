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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.deltav.poller.catalog.Catalog;
import org.deltav.poller.catalog.ServiceDefinition;
import org.deltav.poller.catalog.ServiceResolver;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.poller.ServiceMonitorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

/**
 * Config-gap observability for the flat poller catalog (FR9). Compares the service types present
 * in inventory against the catalog and surfaces every type pollerd cannot schedule, so a gap
 * pages an operator instead of silently unmonitoring a service.
 *
 * <p>A type is <strong>unresolved</strong> (reported on the
 * {@code deltav_pollerd_services_unscheduled{service=...}} gauge) when either:
 * <ul>
 *   <li>no catalog definition matches it (exact or pattern), or</li>
 *   <li>the matching definition's monitor class is absent from the {@link ServiceMonitorRegistry}
 *       — pollerd's {@code PollerRequestBuilderImpl} throws "Monitor not found" for such a class
 *       on every poll, the false-DOWN path this check closes.</li>
 * </ul>
 * Types matched by a definition with {@code enabled: false} are <em>disabled by choice</em> —
 * intentionally not scheduled, so they are excluded from the gauge (D4).
 *
 * <p>At startup exactly one INFO summary line is emitted; the check then re-evaluates on a fixed
 * interval so newly provisioned gap types appear without a restart, re-logging only when the
 * unresolved set changes.
 */
public class CatalogStartupCheck implements DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogStartupCheck.class);

    /** Cap the unresolved-types list in the summary line so a large gap can't flood the log. */
    private static final int MAX_LISTED = 20;
    private static final long RECHECK_INTERVAL_MINUTES = 5;

    private final ServiceResolver resolver;
    private final ServiceMonitorRegistry monitorRegistry;
    private final MonitoredServiceDao monitoredServiceDao;
    private final SessionUtils sessionUtils;
    private final MultiGauge unscheduledGauge;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "pollerd-catalog-gap-check");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean started = new AtomicBoolean(false);

    /** Last unresolved set, in the deterministic case-insensitive order of {@link #recompute}'s TreeSet. */
    private volatile List<String> lastUnresolved = List.of();

    public CatalogStartupCheck(Catalog catalog,
                               ServiceMonitorRegistry monitorRegistry,
                               MonitoredServiceDao monitoredServiceDao,
                               SessionUtils sessionUtils,
                               MeterRegistry meterRegistry) {
        this.resolver = new ServiceResolver(catalog);
        this.monitorRegistry = monitorRegistry;
        this.monitoredServiceDao = monitoredServiceDao;
        this.sessionUtils = sessionUtils;
        this.unscheduledGauge = MultiGauge.builder(PollerdDomainMetrics.SERVICES_UNSCHEDULED)
                .description("Inventory service types pollerd cannot schedule "
                        + "(no catalog definition, or monitor class missing from the registry)")
                .register(meterRegistry);
    }

    /** Initial evaluation once the context is ready (DB reachable), then a periodic recheck. */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        // ApplicationReadyEvent can be published more than once (e.g. a context restart); schedule once.
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // The initial run is guarded too: a transient startup failure must not escape the listener
        // (which would abort context readiness) nor skip scheduling the retry below.
        safeRecompute(true);
        scheduler.scheduleAtFixedRate(() -> safeRecompute(false),
                RECHECK_INTERVAL_MINUTES, RECHECK_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    /** Runs {@link #recompute} swallowing transient failures so the periodic check keeps retrying. */
    private void safeRecompute(boolean alwaysLogSummary) {
        try {
            recompute(alwaysLogSummary);
        } catch (RuntimeException e) {
            LOG.warn("Catalog gap check failed (will retry at next interval): {}", e.getMessage());
        }
    }

    @Override
    public void destroy() {
        scheduler.shutdownNow();
    }

    void recompute(boolean alwaysLogSummary) {
        final Set<String> inventoryTypes = inventoryServiceTypes();
        final Set<String> unresolved = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        int scheduled = 0;

        for (final String type : inventoryTypes) {
            final Optional<ServiceDefinition> match = resolver.resolve(type);
            if (match.isEmpty()) {
                unresolved.add(type);                                   // no catalog definition
                continue;
            }
            final ServiceDefinition def = match.get();
            if (!def.enabled()) {
                continue;                                              // disabled by choice — not a gap (D4)
            }
            if (monitorRegistry.getMonitorByClassName(def.monitor()) == null) {
                unresolved.add(type);                                   // monitor missing → false-DOWN
            } else {
                scheduled++;
            }
        }

        // Rebuild the labeled gauge: one series per unresolved type with value 1.
        final List<MultiGauge.Row<?>> rows = new ArrayList<>(unresolved.size());
        for (final String name : unresolved) {
            rows.add(MultiGauge.Row.of(Tags.of(PollerdDomainMetrics.TAG_SERVICE, name), 1.0));
        }
        unscheduledGauge.register(rows, true);

        // Compare like-for-like ordered lists (both derived from the case-insensitive TreeSet) so the
        // change check can't be skewed by Set type/casing asymmetry.
        final List<String> current = List.copyOf(unresolved);
        final boolean changed = !current.equals(lastUnresolved);
        lastUnresolved = current;
        if (alwaysLogSummary || changed) {
            LOG.info("catalog-summary scheduled={} types={} unresolved={} unresolved-types={}",
                    scheduled, inventoryTypes.size(), unresolved.size(), formatTruncated(unresolved));
        }
    }

    /** Distinct service-type names the engine would schedule (active inventory services). */
    private Set<String> inventoryServiceTypes() {
        return sessionUtils.withReadOnlyTransaction(() ->
                monitoredServiceDao.findAllServicesForScheduling().stream()
                        .map(OnmsMonitoredService::getServiceName)
                        .filter(name -> name != null && !name.isBlank())
                        .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
    }

    private static String formatTruncated(final Set<String> names) {
        final List<String> list = new ArrayList<>(names);
        if (list.size() <= MAX_LISTED) {
            return list.toString();
        }
        // Keep the overflow marker inside the bracketed token so the field stays a single [..] value.
        final List<String> shown = new ArrayList<>(list.subList(0, MAX_LISTED));
        shown.add("(+" + (list.size() - MAX_LISTED) + " more)");
        return shown.toString();
    }
}
