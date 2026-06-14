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

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Supplier;

import org.opennms.netmgt.filter.api.FilterDao;

/**
 * A {@link FilterDao} that replaces {@code JdbcFilterDao}/{@code FilterDaoFactory} in the
 * flat-catalog poller (FR7). The flat model has no filter rules: every monitored interface
 * belongs to the synthetic catalog packages, so this implementation answers the only three
 * methods the frozen {@code PollerConfigManager} actually exercises and rejects the rest.
 *
 * <p><strong>VERIFIED necessity</strong> (adversarial review): the manager builds its
 * package IP map solely from {@link #getActiveIPAddressList(String)} — a data-less stub would
 * mark every service Not-Polled and silence all monitoring. So this returns ALL active
 * inventory IPs (the {@code rule} is ignored) via a constructor-injected supplier; daemon-boot
 * provides an {@code IpInterfaceDao}-backed supplier, keeping this library Spring-free.
 *
 * <p>The active-IP list is cached so the manager's repeated calls during package-map rebuild
 * are cheap; {@link #flushActiveIpAddressListCache()} clears it so a subsequent rebuild picks
 * up newly provisioned interfaces — which is exactly how {@code rebuildPackageIpListMap()}
 * sees new inventory.
 */
public final class InventoryFilterDao implements FilterDao {

    private final Supplier<List<InetAddress>> activeIpSupplier;
    private volatile List<InetAddress> cached;

    /**
     * @param activeIpSupplier supplies all active inventory IP addresses; queried lazily and
     *                         re-queried after {@link #flushActiveIpAddressListCache()}
     */
    public InventoryFilterDao(final Supplier<List<InetAddress>> activeIpSupplier) {
        this.activeIpSupplier = Objects.requireNonNull(activeIpSupplier, "activeIpSupplier");
    }

    /** Returns ALL active inventory IPs; the filter {@code rule} is intentionally ignored. */
    @Override
    public List<InetAddress> getActiveIPAddressList(final String rule) {
        List<InetAddress> snapshot = cached;
        if (snapshot == null) {
            final List<InetAddress> supplied = activeIpSupplier.get();
            if (supplied == null) {
                throw new IllegalStateException("activeIpSupplier returned null (expected a list of active IPs)");
            }
            snapshot = List.copyOf(supplied);
            cached = snapshot;
        }
        return snapshot;
    }

    /** Clears the cache so the next {@link #getActiveIPAddressList(String)} re-queries inventory. */
    @Override
    public void flushActiveIpAddressListCache() {
        cached = null;
    }

    /** Accepts any rule — the flat model has no filter rules to reject. */
    @Override
    public void validateRule(final String rule) {
        // no-op: all rules are trivially valid in the flat catalog model
    }

    // --- Everything below is never invoked by the flat-catalog daemon paths. ---

    @Override
    public SortedMap<Integer, String> getNodeMap(final String rule) {
        throw unsupported("getNodeMap");
    }

    @Override
    public Map<InetAddress, Set<String>> getIPAddressServiceMap(final String rule) {
        throw unsupported("getIPAddressServiceMap");
    }

    @Override
    public Map<Integer, Map<InetAddress, Set<String>>> getNodeIPAddressServiceMap(final String rule) {
        throw unsupported("getNodeIPAddressServiceMap");
    }

    @Override
    public List<InetAddress> getIPAddressList(final String rule) {
        throw unsupported("getIPAddressList");
    }

    @Override
    public boolean isValid(final String addr, final String rule) {
        throw unsupported("isValid");
    }

    @Override
    public boolean isRuleMatching(final String rule) {
        throw unsupported("isRuleMatching");
    }

    private static UnsupportedOperationException unsupported(final String method) {
        return new UnsupportedOperationException(
                "InventoryFilterDao." + method + " is not used by the flat-catalog poller");
    }
}
