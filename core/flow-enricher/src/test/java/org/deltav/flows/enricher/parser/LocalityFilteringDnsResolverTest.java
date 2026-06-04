/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;

class LocalityFilteringDnsResolverTest {

    private final DnsResolver delegate = mock(DnsResolver.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private LocalityFilteringDnsResolver privateOnly() {
        when(delegate.reverseLookup(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.of("host.example")));
        return new LocalityFilteringDnsResolver(delegate, true, registry);
    }

    private static InetAddress v4(String s) throws Exception { return InetAddress.getByName(s); }

    /** Build a genuine 16-byte IPv4-mapped Inet6Address (getByName would collapse to Inet4Address). */
    private static InetAddress v4mapped(int a, int b, int c, int d) throws Exception {
        byte[] bytes = new byte[16];
        bytes[10] = (byte) 0xff;
        bytes[11] = (byte) 0xff;
        bytes[12] = (byte) a; bytes[13] = (byte) b; bytes[14] = (byte) c; bytes[15] = (byte) d;
        return InetAddress.getByAddress(bytes);
    }

    @Test
    void scopePrivate_delegatesForPrivateIpv4() throws Exception {
        var r = privateOnly();
        for (String ip : new String[]{"10.0.0.1", "172.16.5.5", "192.168.1.1", "127.0.0.1", "169.254.1.1"}) {
            r.reverseLookup(v4(ip));
        }
        verify(delegate, times(5)).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isZero();
    }

    @Test
    void scopePrivate_skipsPublicIpv4AndCountsFiltered() throws Exception {
        var r = privateOnly();
        CompletableFuture<Optional<String>> f = r.reverseLookup(v4("8.8.8.8"));
        assertThat(f.join()).isEmpty();
        verify(delegate, never()).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isEqualTo(1.0);
    }

    @Test
    void scopePrivate_ipv4MappedIpv6PrivateIsTreatedAsPrivate() throws Exception {
        var r = privateOnly();
        r.reverseLookup(v4mapped(10, 0, 0, 1));
        r.reverseLookup(v4mapped(192, 168, 1, 1));
        r.reverseLookup(v4mapped(127, 0, 0, 1));
        verify(delegate, times(3)).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isZero();
    }

    @Test
    void scopePrivate_ipv4MappedIpv6PublicIsSkipped() throws Exception {
        var r = privateOnly();
        assertThat(r.reverseLookup(v4mapped(8, 8, 8, 8)).join()).isEmpty();
        verify(delegate, never()).reverseLookup(any());
    }

    @Test
    void scopePrivate_172_32_isPublicAndFiltered() throws Exception {
        var r = privateOnly();
        CompletableFuture<Optional<String>> f = r.reverseLookup(v4("172.32.0.0"));
        assertThat(f.join()).isEmpty();
        verify(delegate, never()).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isEqualTo(1.0);
    }

    @Test
    void scopePrivate_ipv6UlaIsPrivate() throws Exception {
        var r = privateOnly();
        r.reverseLookup(InetAddress.getByName("fc00::1"));
        r.reverseLookup(InetAddress.getByName("fd00::1"));
        verify(delegate, times(2)).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isZero();
    }

    @Test
    void scopePrivate_ipv6LinkLocalIsPrivate() throws Exception {
        var r = privateOnly();
        r.reverseLookup(InetAddress.getByName("fe80::1"));
        verify(delegate, times(1)).reverseLookup(any());
        assertThat(registry.counter("deltav_dns_reverse_lookups_total", "result", "filtered").count()).isZero();
    }

    @Test
    void scopeAll_alwaysDelegates() throws Exception {
        when(delegate.reverseLookup(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.of("h")));
        var r = new LocalityFilteringDnsResolver(delegate, false, registry);
        r.reverseLookup(v4("8.8.8.8"));
        verify(delegate, times(1)).reverseLookup(any());
    }

    @Test
    void forwardLookupAlwaysDelegates() {
        when(delegate.lookup(any())).thenReturn(
                CompletableFuture.completedFuture(Optional.empty()));
        new LocalityFilteringDnsResolver(delegate, true, registry).lookup("example.com");
        verify(delegate, times(1)).lookup("example.com");
    }
}
