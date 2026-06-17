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
import org.opennms.netmgt.dao.api.ApplicationDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.poller.ServiceMonitorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

/**
 * Config-gap observability for the flat poller catalog as consumed by PerspectivePollerd (FR9).
 *
 * <p>Mirrors pollerd's {@code CatalogStartupCheck} but over a different service universe.
 * PerspectivePollerd does not poll all of inventory — it polls the services that are members of an
 * application mapped to one or more perspective locations ({@code ApplicationDao.getServicePerspectives()}).
 * The frozen {@code PerspectivePollerd.onServicePerspectiveAdded} silently drops a perspective service
 * whose name has no matching (enabled) catalog definition or whose monitor class is missing from the
 * {@link ServiceMonitorRegistry}; this check surfaces exactly that set on the
 * {@code deltav_perspectivepollerd_services_unscheduled{service=...}} gauge so the gap is loud instead
 * of silent.</p>
 *
 * <p>A perspective service type is <strong>unresolved</strong> when either:
 * <ul>
 *   <li>no catalog definition matches it (exact or pattern), or</li>
 *   <li>the matching definition's monitor class is absent from the {@link ServiceMonitorRegistry}.</li>
 * </ul>
 * Types matched by a definition with {@code enabled: false} are <em>disabled by choice</em> and
 * excluded from the gauge (D4).</p>
 *
 * <p>At startup exactly one INFO summary line is emitted; the check then re-evaluates on a fixed
 * interval so a newly mapped perspective gap appears without a restart, re-logging only when the
 * unresolved set changes.</p>
 */
public class PerspectiveCatalogStartupCheck implements DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(PerspectiveCatalogStartupCheck.class);

    /** Cap the unresolved-types list in the summary line so a large gap can't flood the log. */
    private static final int MAX_LISTED = 20;
    private static final long RECHECK_INTERVAL_MINUTES = 5;

    private final ServiceResolver resolver;
    private final ServiceMonitorRegistry monitorRegistry;
    private final ApplicationDao applicationDao;
    private final SessionUtils sessionUtils;
    private final MultiGauge unscheduledGauge;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "perspectivepollerd-catalog-gap-check");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean started = new AtomicBoolean(false);

    /** Last unresolved set, in the deterministic case-insensitive order of {@link #recompute}'s TreeSet. */
    private volatile List<String> lastUnresolved = List.of();

    public PerspectiveCatalogStartupCheck(Catalog catalog,
                                          ServiceMonitorRegistry monitorRegistry,
                                          ApplicationDao applicationDao,
                                          SessionUtils sessionUtils,
                                          MeterRegistry meterRegistry) {
        this.resolver = new ServiceResolver(catalog);
        this.monitorRegistry = monitorRegistry;
        this.applicationDao = applicationDao;
        this.sessionUtils = sessionUtils;
        this.unscheduledGauge = MultiGauge.builder(PerspectivePollerdDomainMetrics.SERVICES_UNSCHEDULED)
                .description("Perspective-polled service types (application members) perspectivepollerd "
                        + "cannot schedule (no catalog definition, or monitor class missing from the registry)")
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
            LOG.warn("Perspective catalog gap check failed (will retry at next interval): {}", e.getMessage());
        }
    }

    @Override
    public void destroy() {
        scheduler.shutdownNow();
    }

    void recompute(boolean alwaysLogSummary) {
        final Set<String> perspectiveTypes = perspectiveServiceTypes();
        final Set<String> unresolved = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        int scheduled = 0;

        for (final String type : perspectiveTypes) {
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
                unresolved.add(type);                                   // monitor missing → silent drop
            } else {
                scheduled++;
            }
        }

        // Rebuild the labeled gauge: one series per unresolved type with value 1.
        final List<MultiGauge.Row<?>> rows = new ArrayList<>(unresolved.size());
        for (final String name : unresolved) {
            rows.add(MultiGauge.Row.of(Tags.of(PerspectivePollerdDomainMetrics.TAG_SERVICE, name), 1.0));
        }
        unscheduledGauge.register(rows, true);

        // Compare like-for-like ordered lists (both derived from the case-insensitive TreeSet) so the
        // change check can't be skewed by Set type/casing asymmetry.
        final List<String> current = List.copyOf(unresolved);
        final boolean changed = !current.equals(lastUnresolved);
        lastUnresolved = current;
        if (alwaysLogSummary || changed) {
            LOG.info("perspective-catalog-summary scheduled={} perspective-services={} unresolved={} unresolved-types={}",
                    scheduled, perspectiveTypes.size(), unresolved.size(), formatTruncated(unresolved));
        }
    }

    /**
     * Distinct service-type names PerspectivePollerd would schedule: the services that are members of
     * an application mapped to at least one perspective location. Read inside a read-only transaction
     * so the lazy {@code service.serviceType} relation resolves.
     */
    private Set<String> perspectiveServiceTypes() {
        return sessionUtils.withReadOnlyTransaction(() ->
                applicationDao.getServicePerspectives().stream()
                        .map(sp -> sp.getService().getServiceName())
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
