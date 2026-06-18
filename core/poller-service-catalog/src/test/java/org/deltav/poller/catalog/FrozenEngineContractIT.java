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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.config.PollerConfigFactory;
import org.opennms.netmgt.config.poller.Package;
import org.opennms.netmgt.config.poller.PollerConfiguration;

/**
 * D8 contract suite: pins translator output through the REAL frozen {@code PollerConfigFactory}
 * (never mocking {@code PollerConfig}), with JARs resolved via {@code ${deltav.horizon.version}}
 * so a horizon bump re-proves the adapter. Constructed directly from the in-memory synthetic
 * {@link PollerConfiguration} + {@link InventoryFilterDao} via
 * {@code PollerConfigFactory(long, PollerConfiguration, FilterDao)} — confirming the D3 strategy
 * end-to-end.
 *
 * <p>Pins covered here are the config-layer behaviors. The remaining behavioral pins that need
 * the full poller.impl runtime (PollableServiceConfig downtime-reschedule of a down service,
 * Poller.scheduleService {@code 'N'}-marking + {@code 'N'->'A'} restoration, pattern-variable
 * flow through a live poll) are tracked in deferred-work for completion within Story 1.6.
 */
class FrozenEngineContractIT {

    private final CatalogTranslator translator = new CatalogTranslator();

    /** Empty-catalog boot tolerance: the real factory constructs with no packages and no throw. */
    @Test
    void emptyCatalogBootTolerance() {
        assertDoesNotThrow(() -> factory(new Catalog(List.of()), List.of()));
    }

    /** Package re-fetch by stable name: PollableServiceConfig re-obtains its package by name. */
    @Test
    void packageReFetchByStableName() throws UnknownHostException {
        final PollerConfigFactory factory = factory(
                catalog(def("ICMP", null, "org.opennms.netmgt.poller.monitors.IcmpMonitor", 300000)),
                List.of(addr("10.0.0.1")));

        final Package pkg = factory.getPackage("catalog-ICMP");
        assertNotNull(pkg, "synthetic package must be re-fetchable by its stable name");
        assertEquals("catalog-ICMP", pkg.getName());
    }

    /** isInterfaceInPackage matches via InventoryFilterDao-supplied IPs (VERIFIED necessity). */
    @Test
    void isInterfaceInPackageMatchesViaInventoryFilterDao() throws UnknownHostException {
        final PollerConfigFactory factory = factory(
                catalog(def("ICMP", null, "org.opennms.netmgt.poller.monitors.IcmpMonitor", 300000)),
                List.of(addr("10.0.0.1"), addr("10.0.0.2")));

        final Package pkg = factory.getPackage("catalog-ICMP");
        assertTrue(factory.isInterfaceInPackage("10.0.0.1", pkg));
        assertTrue(factory.isInterfaceInPackage("10.0.0.2", pkg));
    }

    /** With no inventory IPs the package matches nothing — proving the data-less-stub failure mode. */
    @Test
    void emptyInventoryYieldsNoPackageMatch() throws UnknownHostException {
        final PollerConfigFactory factory = factory(
                catalog(def("ICMP", null, "org.opennms.netmgt.poller.monitors.IcmpMonitor", 300000)),
                List.of());

        final Package pkg = factory.getPackage("catalog-ICMP");
        assertFalse(factory.isInterfaceInPackage("10.0.0.1", pkg),
                "an empty active-IP list must not match — this is why InventoryFilterDao must carry data");
    }

    /** Per-package downtime is present in the real manager's config: begin=0, interval=service interval. */
    @Test
    void perPackageDowntimeSynthesized() throws UnknownHostException {
        final Package pkg = factory(
                catalog(def("ICMP", null, "org.opennms.netmgt.poller.monitors.IcmpMonitor", 300000)),
                List.of(addr("10.0.0.1"))).getPackage("catalog-ICMP");

        assertEquals(1, pkg.getDowntimes().size());
        assertEquals(0L, pkg.getDowntimes().get(0).getBegin());
        assertEquals(300000L, pkg.getDowntimes().get(0).getInterval());
    }

    /** Resolver parity: ServiceResolver's verdict equals the frozen manager's on every fixture name. */
    @Test
    void resolverParityWithFrozenManager() throws UnknownHostException {
        final Catalog catalog = catalog(
                def("HTTP-8080", null, "org.opennms.netmgt.poller.monitors.HttpMonitor", 30000),
                def("HttpFamily", "^HTTP-(\\d+)$", "org.opennms.netmgt.poller.monitors.PageSequenceMonitor", 30000));
        final ServiceResolver resolver = new ServiceResolver(catalog);
        final PollerConfigFactory factory = factory(catalog, List.of(addr("10.0.0.1")));

        // Includes a case-variant ("http-8080") to pin that ServiceResolver matches the frozen
        // manager's case-insensitive exact match — the FR9 gauge must agree with what the engine schedules.
        for (final String name : List.of("HTTP-8080", "http-8080", "HTTP-9090", "SNMP")) {
            final String resolverVerdict = resolver.resolve(name)
                    .map(d -> "catalog-" + d.name()).orElse(null);
            final String managerVerdict = factory.getPackages().stream()
                    .filter(p -> factory.isServiceInPackageAndEnabled(name, p))
                    .map(Package::getName)
                    .findFirst().orElse(null);
            assertEquals(resolverVerdict, managerVerdict, "resolver parity for '" + name + "'");
        }
    }

    /** update() no-ops under the setPollerConfigFile/version trick — XML is never reloaded over the synthetic config. */
    @Test
    void updateNoOpsUnderVersionTrick() throws Exception {
        final File configFile = Files.createTempFile("poller-services", ".yaml").toFile();
        configFile.deleteOnExit();
        PollerConfigFactory.setPollerConfigFile(configFile);

        final PollerConfiguration config = translator.translate(
                catalog(def("ICMP", null, "org.opennms.netmgt.poller.monitors.IcmpMonitor", 300000)),
                EngineSettings.defaults());
        // currentVersion == file's lastModified -> update()'s `lastModified > version` guard is false,
        // so it never tries to parse the (non-XML) yaml file and keeps the synthetic config.
        final PollerConfigFactory factory =
                new PollerConfigFactory(configFile.lastModified(), config, filterDao(List.of(addr("10.0.0.1"))));

        assertDoesNotThrow(factory::update);
        assertNotNull(factory.getPackage("catalog-ICMP"), "synthetic config must survive update()");
    }

    /**
     * Node-outage methods on the real manager must not NPE. The frozen engine dereferences
     * getNodeOutage() with no null guard (isNodeOutageProcessingEnabled/getCriticalService and, on
     * the core poll path, PollableInterface.poll), so the translator must emit the disabled element.
     */
    @Test
    void nodeOutageMethodsDoNotThrowOnSyntheticConfig() throws UnknownHostException {
        final PollerConfigFactory factory = factory(
                catalog(def("ICMP", null, "org.opennms.netmgt.poller.monitors.IcmpMonitor", 300000)),
                List.of(addr("10.0.0.1")));

        assertFalse(factory.isNodeOutageProcessingEnabled(), "node-outage processing is off in delta-v");
        assertEquals("ICMP", factory.getCriticalService());
        assertDoesNotThrow(factory::shouldPollAllIfNoCriticalServiceDefined);
    }

    /**
     * The translator emits no {@code <rrd>} block (delta-v's Pollerd uses a no-op persister). The
     * frozen engine's status-storing path calls {@code PollerConfigManager.getStep(pkg)} /
     * {@code getRRAList(pkg)} on every poll result; as of horizon 1.0.19 those null-guard
     * {@code getRrd()} and return a default step (300) / empty RRA list rather than NPE-ing and
     * force-marking the service DOWN. This pins that the synthetic config (no rrd) + the engine
     * guard resolve a usable step — so the parity {@code <rrd>} workaround can stay removed (#362).
     */
    @Test
    void rrdStepResolvesOnSyntheticConfig() throws UnknownHostException {
        final PollerConfigFactory factory = factory(
                catalog(def("ICMP", null, "org.opennms.netmgt.poller.monitors.IcmpMonitor", 300000)),
                List.of(addr("10.0.0.1")));
        final Package pkg = factory.getPackage("catalog-ICMP");

        // getStep() is the exact call that NPE'd at runtime; the engine guard now returns the default.
        assertEquals(300, factory.getStep(pkg), "engine returns the default step when no <rrd> is present");
        assertDoesNotThrow(() -> factory.getRRAList(pkg), "rra list must resolve without NPE");
    }

    // --- helpers ---

    private PollerConfigFactory factory(final Catalog catalog, final List<InetAddress> ips) {
        final PollerConfiguration config = translator.translate(catalog, EngineSettings.defaults());
        return new PollerConfigFactory(1L, config, filterDao(ips));
    }

    private static InventoryFilterDao filterDao(final List<InetAddress> ips) {
        return new InventoryFilterDao(() -> ips);
    }

    private static Catalog catalog(final ServiceDefinition... defs) {
        return new Catalog(List.of(defs));
    }

    private static ServiceDefinition def(final String name, final String pattern,
                                         final String monitor, final int interval) {
        return new ServiceDefinition(name, pattern, monitor, interval, Boolean.TRUE, Map.of());
    }

    private static InetAddress addr(final String ip) throws UnknownHostException {
        return InetAddress.getByName(ip);
    }
}
