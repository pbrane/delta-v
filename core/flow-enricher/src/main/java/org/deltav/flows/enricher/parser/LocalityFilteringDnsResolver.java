/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.micrometer.core.instrument.MeterRegistry;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;

/**
 * A {@link DnsResolver} decorator that, when {@code privateOnly} is set,
 * only reverse-resolves IPs in private/local ranges (RFC1918, IPv6 ULA,
 * loopback, link-local) and short-circuits everything else to an empty
 * result — bounding reverse-DNS cardinality to the operator's own address
 * space. When {@code privateOnly} is false, every address is delegated
 * (Horizon-equivalent behaviour).
 *
 * <p>Flow addresses arrive as IPv6 (the ClickHouse {@code flows_raw.*_address}
 * columns are IPv6), so a private IPv4 can reach us as an IPv4-mapped
 * {@code Inet6Address} (e.g. {@code ::ffff:10.0.0.1}). {@link InetAddress}'s
 * {@code isSiteLocalAddress()} checks the IPv6 prefix and would mis-classify
 * such an address as public; we therefore normalize IPv4-mapped IPv6 to the
 * embedded IPv4 before testing.
 */
public class LocalityFilteringDnsResolver implements DnsResolver {

    private static final String METRIC = "deltav_dns_reverse_lookups_total";

    private final DnsResolver delegate;
    private final boolean privateOnly;
    private final MeterRegistry registry;

    public LocalityFilteringDnsResolver(DnsResolver delegate, boolean privateOnly, MeterRegistry registry) {
        this.delegate = Objects.requireNonNull(delegate);
        this.privateOnly = privateOnly;
        this.registry = Objects.requireNonNull(registry);
    }

    @Override
    public CompletableFuture<Optional<InetAddress>> lookup(String hostname) {
        return delegate.lookup(hostname);
    }

    @Override
    public CompletableFuture<Optional<String>> reverseLookup(InetAddress address) {
        if (privateOnly && !isPrivate(address)) {
            registry.counter(METRIC, "result", "filtered").increment();
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return delegate.reverseLookup(address).whenComplete((result, ex) -> {
            final String tag = ex != null ? "error" : (result != null && result.isPresent() ? "hit" : "miss");
            registry.counter(METRIC, "result", tag).increment();
        });
    }

    static boolean isPrivate(InetAddress address) {
        final InetAddress a = normalizeMapped(address);
        if (a.isSiteLocalAddress() || a.isLoopbackAddress() || a.isLinkLocalAddress()) {
            return true;
        }
        final byte[] b = a.getAddress();
        // IPv6 Unique Local Address fc00::/7 (Java has no isUniqueLocalAddress()).
        return b.length == 16 && (b[0] & 0xFE) == 0xFC;
    }

    /** Unwrap an IPv4-mapped IPv6 address (::ffff:a.b.c.d) to its embedded Inet4Address. */
    static InetAddress normalizeMapped(InetAddress address) {
        final byte[] b = address.getAddress();
        if (b.length == 16 && isV4Mapped(b)) {
            try {
                return InetAddress.getByAddress(Arrays.copyOfRange(b, 12, 16));
            } catch (UnknownHostException e) {
                return address; // 4-byte array is always valid; unreachable in practice
            }
        }
        return address;
    }

    private static boolean isV4Mapped(byte[] b) {
        for (int i = 0; i < 10; i++) {
            if (b[i] != 0) {
                return false;
            }
        }
        return b[10] == (byte) 0xff && b[11] == (byte) 0xff;
    }
}
