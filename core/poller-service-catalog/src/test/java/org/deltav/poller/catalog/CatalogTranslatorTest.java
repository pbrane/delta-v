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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.config.poller.Downtime;
import org.opennms.netmgt.config.poller.Monitor;
import org.opennms.netmgt.config.poller.Package;
import org.opennms.netmgt.config.poller.PollerConfiguration;
import org.opennms.netmgt.config.poller.Service;

/**
 * Story 1.4 acceptance criteria for {@link CatalogTranslator}: one package per definition with
 * synthesized downtime, exact-before-pattern ordering, monitor bindings, engine scalars on the
 * root, disabled definitions omitted, and {@code nextOutageId} left unset.
 */
class CatalogTranslatorTest {

    private final CatalogTranslator translator = new CatalogTranslator();

    @Test
    void translatesPackagesDowntimeServicesAndMonitors() {
        final Catalog catalog = new Catalog(List.of(
                new ServiceDefinition("ICMP", null, "a.b.IcmpMonitor", 300000, true,
                        Map.of("retry", "2")),
                new ServiceDefinition("HttpFamily", "^HTTP-(\\d+)$", "a.b.PageSequenceMonitor",
                        30000, true, Map.of())));

        final PollerConfiguration config = translator.translate(catalog, EngineSettings.defaults());

        assertEquals(2, config.getPackages().size());

        final Package icmp = config.getPackage("catalog-ICMP");
        assertEquals(1, icmp.getDowntimes().size());
        final Downtime downtime = icmp.getDowntimes().get(0);
        assertEquals(0L, downtime.getBegin());
        assertEquals(300000L, downtime.getInterval());

        final Service icmpSvc = icmp.getServices().get(0);
        assertEquals("ICMP", icmpSvc.getName());
        assertEquals(300000L, icmpSvc.getInterval());
        assertEquals("on", icmpSvc.getStatus());
        assertNull(icmpSvc.getPattern());
        assertEquals("2", icmpSvc.getParameterMap().get("retry"));

        final Service httpSvc = config.getPackage("catalog-HttpFamily").getServices().get(0);
        assertEquals("^HTTP-(\\d+)$", httpSvc.getPattern());

        // Monitor bindings: one per definition, keyed by the definition name.
        assertEquals(2, config.getMonitors().size());
        final Monitor icmpMonitor = config.getMonitors().stream()
                .filter(m -> "ICMP".equals(m.getService())).findFirst().orElseThrow();
        assertEquals("a.b.IcmpMonitor", icmpMonitor.getClassName());
    }

    @Test
    void emitsExactPackagesBeforePatternPackages() {
        final Catalog catalog = new Catalog(List.of(
                new ServiceDefinition("PatternA", "^A-(\\d+)$", "a.b.M1", 30000, true, Map.of()),
                new ServiceDefinition("ExactB", null, "a.b.M2", 30000, true, Map.of()),
                new ServiceDefinition("PatternC", "^C-(\\d+)$", "a.b.M3", 30000, true, Map.of()),
                new ServiceDefinition("ExactD", null, "a.b.M4", 30000, true, Map.of())));

        final List<String> packageNames = translator.translate(catalog, EngineSettings.defaults())
                .getPackages().stream().map(Package::getName).toList();

        // Exact definitions first (in file order), then pattern definitions (in file order).
        assertEquals(List.of("catalog-ExactB", "catalog-ExactD", "catalog-PatternA", "catalog-PatternC"),
                packageNames);
    }

    @Test
    void omitsDisabledDefinitionsEntirely() {
        final Catalog catalog = new Catalog(List.of(
                new ServiceDefinition("Live", null, "a.b.M1", 30000, true, Map.of()),
                new ServiceDefinition("Killed", null, "a.b.M2", 30000, false, Map.of())));

        final PollerConfiguration config = translator.translate(catalog, EngineSettings.defaults());

        assertEquals(1, config.getPackages().size());
        assertNull(config.getPackage("catalog-Killed"), "disabled definition must be omitted");
        assertTrue(config.getMonitors().stream().noneMatch(m -> "Killed".equals(m.getService())));
    }

    @Test
    void failsFastWhenAnEnabledDefinitionMissesALoadBearingField() {
        // A catalog that bypassed validation (null interval) must fail with a clear message, not an NPE.
        final Catalog badInterval = new Catalog(List.of(
                new ServiceDefinition("ICMP", null, "a.b.IcmpMonitor", null, true, Map.of())));
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> translator.translate(badInterval, EngineSettings.defaults()));
        assertTrue(ex.getMessage().contains("interval"), ex.getMessage());
    }

    @Test
    void mapsEngineSettingsAndDisablesAdapterConstantsAndLeavesNextOutageIdUnset() {
        final PollerConfiguration config = translator.translate(
                new Catalog(List.of()), new EngineSettings(12, true, 50));

        assertEquals(12, config.getThreads());
        assertEquals(Boolean.TRUE, config.getAsyncPollingEngineEnabled());
        assertEquals(50, config.getMaxConcurrentAsyncPolls());
        assertEquals("false", config.getServiceUnresponsiveEnabled());
        assertEquals("false", config.getPathOutageEnabled());
        // nextOutageId dropped (OVI #3): translator never sets it, so the JAXB default is left
        // untouched. Assert against a freshly-constructed default rather than a hardcoded string,
        // so this pins "translator does not interfere" without coupling to the frozen model's literal.
        assertEquals(new PollerConfiguration().getNextOutageId(), config.getNextOutageId(),
                "translator must not set nextOutageId; the JAXB default must remain unchanged");
        assertTrue(config.getPackages().isEmpty(), "empty catalog yields no packages");
    }
}
