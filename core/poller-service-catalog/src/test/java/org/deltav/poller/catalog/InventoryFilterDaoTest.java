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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

/**
 * Story 1.4 acceptance criteria for {@link InventoryFilterDao}: three real methods, the rest
 * throw {@link UnsupportedOperationException}.
 */
class InventoryFilterDaoTest {

    /** AC: getActiveIPAddressList returns all supplied IPs, ignoring the rule. */
    @Test
    void returnsAllSuppliedIpsIgnoringRule() throws UnknownHostException {
        final List<InetAddress> ips = List.of(
                InetAddress.getByName("10.0.0.1"), InetAddress.getByName("10.0.0.2"));
        final InventoryFilterDao dao = new InventoryFilterDao(() -> ips);

        assertEquals(ips, dao.getActiveIPAddressList("any rule is ignored"));
        assertEquals(ips, dao.getActiveIPAddressList(null));
    }

    /** AC: flushActiveIpAddressListCache triggers a re-query of the supplier. */
    @Test
    void flushTriggersRequery() throws UnknownHostException {
        final InetAddress a = InetAddress.getByName("10.0.0.1");
        final InetAddress b = InetAddress.getByName("10.0.0.2");
        final AtomicInteger calls = new AtomicInteger();
        final Supplier<List<InetAddress>> supplier = () ->
                calls.getAndIncrement() == 0 ? List.of(a) : List.of(a, b);

        final InventoryFilterDao dao = new InventoryFilterDao(supplier);

        final List<InetAddress> first = dao.getActiveIPAddressList(null);
        assertEquals(List.of(a), first);
        assertSame(first, dao.getActiveIPAddressList(null), "second call is cached, no re-query");
        assertEquals(1, calls.get());

        dao.flushActiveIpAddressListCache();
        assertEquals(List.of(a, b), dao.getActiveIPAddressList(null), "re-query after flush");
        assertEquals(2, calls.get());
    }

    /** AC: validateRule accepts (no throw). */
    @Test
    void validateRuleAccepts() {
        new InventoryFilterDao(List::of).validateRule("anything");
    }

    /** A supplier that returns null fails with a clear message rather than a bare NPE. */
    @Test
    void nullSupplierResultFailsClearly() {
        final InventoryFilterDao dao = new InventoryFilterDao(() -> null);
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> dao.getActiveIPAddressList(null));
        assertTrue(ex.getMessage().contains("returned null"), ex.getMessage());
    }

    /** AC: every other FilterDao method throws UnsupportedOperationException. */
    @Test
    void unusedMethodsThrow() {
        final InventoryFilterDao dao = new InventoryFilterDao(List::of);
        assertThrows(UnsupportedOperationException.class, () -> dao.getNodeMap("r"));
        assertThrows(UnsupportedOperationException.class, () -> dao.getIPAddressServiceMap("r"));
        assertThrows(UnsupportedOperationException.class, () -> dao.getNodeIPAddressServiceMap("r"));
        assertThrows(UnsupportedOperationException.class, () -> dao.getIPAddressList("r"));
        assertThrows(UnsupportedOperationException.class, () -> dao.isValid("10.0.0.1", "r"));
        assertThrows(UnsupportedOperationException.class, () -> dao.isRuleMatching("r"));
    }
}
