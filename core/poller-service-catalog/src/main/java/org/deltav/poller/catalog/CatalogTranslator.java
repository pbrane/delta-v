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
package org.deltav.poller.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opennms.netmgt.config.poller.CriticalService;
import org.opennms.netmgt.config.poller.Downtime;
import org.opennms.netmgt.config.poller.NodeOutage;
import org.opennms.netmgt.config.poller.Package;
import org.opennms.netmgt.config.poller.PollerConfiguration;
import org.opennms.netmgt.config.poller.Service;

/**
 * Translates the flat catalog into the synthetic JAXB {@link PollerConfiguration} the frozen
 * poller engine consumes (D3, amended). This is the ONLY class that touches
 * {@code org.opennms.netmgt.config.poller.*} — the slice-2 deletion surface.
 *
 * <p>Model: <strong>one synthetic package per enabled definition</strong>, named
 * {@code catalog-<name>}, each carrying its own downtime entry ({@code begin=0,
 * interval=<that service's interval>}). Downtime is package-scoped in the frozen engine, so
 * per-service intervals require per-definition packages; the synthesized downtime keeps a
 * down service rescheduling at its own interval (FR5), invisible to operators.
 *
 * <p>Exact definitions are emitted <strong>before</strong> pattern definitions so the frozen
 * manager's first-match iteration honors D2 precedence (exact beats pattern). Disabled
 * definitions are omitted entirely — the engine then marks affected inventory rows
 * {@code 'N'} (D4 {@code enabled:false} semantics). Engine scalars come from
 * {@link EngineSettings}; {@code serviceUnresponsive}/{@code pathOutage} are disabled
 * adapter constants; {@code nextOutageId} is left unset (dead — Open Verification Item #3).
 *
 * <p><strong>Precondition:</strong> the catalog must have passed {@link CatalogValidator}
 * with no ERRORs (the daemon loader enforces this before calling translate). As
 * defense-in-depth, translate fail-fasts with a clear message if a load-bearing field of an
 * enabled definition is missing, rather than emitting a corrupt config or throwing a bare
 * {@link NullPointerException}.
 */
public final class CatalogTranslator {

    static final String PACKAGE_PREFIX = "catalog-";
    private static final String SERVICE_STATUS_ON = "on";
    private static final String DISABLED = "false";
    /** Catch-all filter; content is irrelevant because {@link InventoryFilterDao} ignores the rule. */
    private static final String CATCH_ALL_FILTER = "IPADDR != '0.0.0.0'";

    /**
     * Builds the synthetic configuration. Output is fed to the frozen {@code PollerConfigFactory}
     * by daemon-boot (Story 2.2) and pinned through the real manager by the contract ITs.
     *
     * @param catalog  the validated catalog
     * @param settings engine-tuning scalars bound from application.yml
     * @return the synthetic {@link PollerConfiguration}
     */
    public PollerConfiguration translate(final Catalog catalog, final EngineSettings settings) {
        final PollerConfiguration config = new PollerConfiguration();
        config.setThreads(settings.threads());
        config.setAsyncPollingEngineEnabled(settings.asyncPollingEngineEnabled());
        config.setMaxConcurrentAsyncPolls(settings.maxConcurrentAsyncPolls());
        config.setServiceUnresponsiveEnabled(DISABLED);
        config.setPathOutageEnabled(DISABLED);
        config.setNodeOutage(disabledNodeOutage());
        // nextOutageId intentionally left unset (dead; JAXB default applies harmlessly).

        for (final ServiceDefinition def : orderedExactBeforePattern(catalog)) {
            config.addPackage(toPackage(def));
            config.addMonitor(def.name(), def.monitor());
        }
        return config;
    }

    /**
     * Node-outage processing is disabled in delta-v ({@code status="off"}), but the element must be
     * <em>present</em>: the frozen engine dereferences {@code getNodeOutage()} with no null guard —
     * {@code PollerConfigManager.isNodeOutageProcessingEnabled()/getCriticalService()} and, on the
     * core poll path, {@code PollableInterface.poll()} — so an absent element NPEs on the first poll.
     * This mirrors the legacy {@code poller-configuration.xml} node-outage block exactly (status off,
     * pollAll true, critical service ICMP), preserving behavior while keeping the engine non-null.
     */
    private static NodeOutage disabledNodeOutage() {
        final NodeOutage nodeOutage = new NodeOutage();
        nodeOutage.setStatus("off");
        nodeOutage.setPollAllIfNoCriticalServiceDefined("true");
        nodeOutage.setCriticalService(new CriticalService("ICMP"));
        return nodeOutage;
    }

    /** Enabled definitions only, exact ones first then pattern ones, each preserving file order. */
    private static List<ServiceDefinition> orderedExactBeforePattern(final Catalog catalog) {
        final List<ServiceDefinition> ordered = new ArrayList<>();
        for (final ServiceDefinition def : catalog.services()) {
            if (def.enabled() && !def.isPattern()) {
                ordered.add(def);
            }
        }
        for (final ServiceDefinition def : catalog.services()) {
            if (def.enabled() && def.isPattern()) {
                ordered.add(def);
            }
        }
        return ordered;
    }

    private static Package toPackage(final ServiceDefinition def) {
        requireTranslatable(def);
        final Package pkg = new Package(PACKAGE_PREFIX + def.name());
        pkg.setFilter(CATCH_ALL_FILTER);

        final Downtime downtime = new Downtime();
        downtime.setBegin(0L);
        downtime.setInterval(def.interval().longValue());
        pkg.addDowntime(downtime);

        // No <rrd> block is emitted. delta-v's Pollerd uses a no-op PersisterFactory (no RRD I/O),
        // and the frozen engine null-guards getRrd() as of horizon 1.0.19 — PollerConfigManager
        // .getStep()/getRRAList() return a default step / empty RRA list when <rrd> is absent.
        // (Earlier horizon versions NPE'd on every poll here, force-marking services DOWN; that
        // parity-only <rrd> workaround was removed once the engine guard shipped. See #362.)
        pkg.addService(toService(def));
        return pkg;
    }

    /**
     * Enforces the load-bearing fields of an enabled definition so a catalog that bypassed
     * validation fails with a clear message instead of an NPE or a silently corrupt config.
     */
    private static void requireTranslatable(final ServiceDefinition def) {
        if (def.name() == null || def.name().isBlank()) {
            throw new IllegalArgumentException(
                    "translate() requires a validated catalog: a service definition has a blank name");
        }
        if (def.interval() == null) {
            throw new IllegalArgumentException(
                    "translate() requires a validated catalog: service '" + def.name() + "' has no interval");
        }
        if (def.monitor() == null || def.monitor().isBlank()) {
            throw new IllegalArgumentException(
                    "translate() requires a validated catalog: service '" + def.name() + "' has no monitor");
        }
    }

    private static Service toService(final ServiceDefinition def) {
        final Service service = new Service();
        service.setName(def.name());
        if (def.isPattern()) {
            service.setPattern(def.pattern());
        }
        service.setInterval(def.interval());
        service.setStatus(SERVICE_STATUS_ON);
        for (final Map.Entry<String, String> param : def.parameters().entrySet()) {
            // Verbatim: keys and values are never translated (foreign monitor contract).
            service.addParameter(param.getKey(), param.getValue());
        }
        return service;
    }
}
