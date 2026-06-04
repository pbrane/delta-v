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
package org.deltav.flows.enricher.parser;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DefaultDnsRecordDecoder;
import io.netty.handler.codec.dns.DnsPtrRecord;
import io.netty.handler.codec.dns.DnsRawRecord;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsResponse;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.SequentialDnsServerAddressStreamProvider;
import io.netty.util.ReferenceCountUtil;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Delta-V-native async reverse-DNS resolver backed by Netty 4.2 and
 * resilience4j 2.x.  Replaces the horizon {@code NettyDnsResolver} (which
 * is compiled against resilience4j 1.x and uses the removed
 * {@code ringBufferSizeInHalfOpenState/Closed} API).
 *
 * <p>Configuration is driven entirely by {@link FlowEnricherDnsProperties}.
 * Call {@link #init()} once before use and {@link #close()} on shutdown.
 */
public class DeltavNettyDnsResolver implements DnsResolver {

    private static final Logger LOG = LoggerFactory.getLogger(DeltavNettyDnsResolver.class);

    private final FlowEnricherDnsProperties props;

    // Initialised in init(); all fields final after that point.
    private volatile MultiThreadIoEventLoopGroup group;
    private volatile DnsNameResolver resolver;
    private volatile CircuitBreaker breaker;    // null when circuitBreaker.enabled() == false
    private volatile Bulkhead bulkhead;
    private volatile Cache<InetAddress, Optional<String>> reverseCache;

    public DeltavNettyDnsResolver(FlowEnricherDnsProperties props) {
        this.props = props;
    }

    /**
     * Builds the Netty event-loop group, DNS resolver, resilience4j guards,
     * and Caffeine cache.  Called by Spring via {@code initMethod = "init"}.
     */
    public void init() {
        group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

        DnsNameResolverBuilder builder = new DnsNameResolverBuilder(group.next())
                .datagramChannelType(NioDatagramChannel.class)
                .queryTimeoutMillis(props.queryTimeoutMs());

        if (!props.nameservers().isBlank()) {
            List<InetSocketAddress> servers = parseNameservers(props.nameservers());
            builder.nameServerProvider(new SequentialDnsServerAddressStreamProvider(servers));
        }

        resolver = builder.build();

        if (props.circuitBreaker().enabled()) {
            CircuitBreakerConfig breakerConfig = CircuitBreakerConfig.custom()
                    .slidingWindowSize(100)
                    .minimumNumberOfCalls(10)
                    .failureRateThreshold(props.circuitBreaker().failureRateThreshold())
                    .waitDurationInOpenState(Duration.ofSeconds(15))
                    .permittedNumberOfCallsInHalfOpenState(10)
                    .build();
            breaker = CircuitBreaker.of("deltav-dns", breakerConfig);
        }

        BulkheadConfig bulkheadConfig = BulkheadConfig.custom()
                .maxConcurrentCalls(props.maxConcurrent())
                .maxWaitDuration(Duration.ofMillis(props.queryTimeoutMs() + 100))
                .build();
        bulkhead = Bulkhead.of("deltav-dns", bulkheadConfig);

        long effectiveMaxTtl = props.cache().maxTtlS() > 0 ? props.cache().maxTtlS() : 3600L;
        reverseCache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofSeconds(effectiveMaxTtl))
                .build();

        LOG.info("DeltavNettyDnsResolver initialised: nameservers='{}', queryTimeoutMs={}, maxConcurrent={}, " +
                        "circuitBreakerEnabled={}, cacheTtlS={}",
                props.nameservers().isBlank() ? "<platform default>" : props.nameservers(),
                props.queryTimeoutMs(),
                props.maxConcurrent(),
                props.circuitBreaker().enabled(),
                effectiveMaxTtl);
    }

    /**
     * Shuts down the Netty resolver and event-loop group gracefully.
     * Called by Spring via {@code destroyMethod = "close"}.
     */
    public void close() {
        if (resolver != null) {
            resolver.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    // -----------------------------------------------------------------------
    // DnsResolver contract
    // -----------------------------------------------------------------------

    @Override
    public CompletableFuture<Optional<String>> reverseLookup(InetAddress addr) {
        // Cache hit — short-circuit before acquiring any permits.
        Optional<String> hit = reverseCache.getIfPresent(addr);
        if (hit != null) {
            return CompletableFuture.completedFuture(hit);
        }

        // Circuit-breaker gate.
        boolean breakerAcquired = false;
        if (breaker != null) {
            if (!breaker.tryAcquirePermission()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            breakerAcquired = true;
        }

        // Bulkhead gate.
        if (!bulkhead.tryAcquirePermission()) {
            if (breakerAcquired) {
                breaker.releasePermission();
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }

        final boolean breakerPermitHeld = breakerAcquired;
        final String ptrName = reversePointer(addr);
        final long startNanos = System.nanoTime();
        final CompletableFuture<Optional<String>> future = new CompletableFuture<>();

        io.netty.util.concurrent.Future<AddressedEnvelope<DnsResponse, InetSocketAddress>> queryFuture =
                resolver.query(new DefaultDnsQuestion(ptrName, DnsRecordType.PTR));

        queryFuture.addListener(nettyFuture -> {
            try {
                if (nettyFuture.isSuccess()) {
                    @SuppressWarnings("unchecked")
                    AddressedEnvelope<DnsResponse, InetSocketAddress> envelope =
                            (AddressedEnvelope<DnsResponse, InetSocketAddress>) nettyFuture.getNow();
                    DnsResponse response = envelope.content();
                    try {
                        Optional<String> result = extractPtrName(response);
                        long elapsedNanos = System.nanoTime() - startNanos;
                        if (breakerPermitHeld) {
                            breaker.onSuccess(elapsedNanos, TimeUnit.NANOSECONDS);
                        }
                        bulkhead.onComplete();
                        reverseCache.put(addr, result);
                        future.complete(result);
                    } finally {
                        ReferenceCountUtil.release(response);
                    }
                } else {
                    long elapsedNanos = System.nanoTime() - startNanos;
                    Throwable cause = nettyFuture.cause();
                    if (breakerPermitHeld) {
                        breaker.onError(elapsedNanos, TimeUnit.NANOSECONDS, cause);
                    }
                    bulkhead.onComplete();
                    reverseCache.put(addr, Optional.empty());
                    future.complete(Optional.empty());
                }
            } catch (Exception ex) {
                LOG.debug("Unexpected error processing DNS response for {}", ptrName, ex);
                long elapsedNanos = System.nanoTime() - startNanos;
                if (breakerPermitHeld) {
                    breaker.onError(elapsedNanos, TimeUnit.NANOSECONDS, ex);
                }
                bulkhead.onComplete();
                future.complete(Optional.empty());
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<Optional<InetAddress>> lookup(String hostname) {
        CompletableFuture<Optional<InetAddress>> future = new CompletableFuture<>();
        resolver.resolve(hostname).addListener(nettyFuture -> {
            try {
                if (nettyFuture.isSuccess()) {
                    future.complete(Optional.ofNullable((InetAddress) nettyFuture.getNow()));
                } else {
                    future.complete(Optional.empty());
                }
            } catch (Exception ex) {
                future.complete(Optional.empty());
            }
        });
        return future;
    }

    // -----------------------------------------------------------------------
    // Helpers — package-private to allow unit testing without init()
    // -----------------------------------------------------------------------

    /**
     * Builds the reverse-pointer DNS name for {@code addr}.
     *
     * <ul>
     *   <li>IPv4 (including IPv4-mapped IPv6 {@code ::ffff:a.b.c.d}) →
     *       {@code d.c.b.a.in-addr.arpa}</li>
     *   <li>IPv6 → reversed nibble form ending in {@code .ip6.arpa}</li>
     * </ul>
     */
    static String reversePointer(InetAddress addr) {
        byte[] raw = addr.getAddress();

        // Unwrap IPv4-mapped IPv6 (::ffff:0:0/96 — 16-byte address whose first
        // 10 bytes are 0x00 and bytes 10-11 are 0xff 0xff).
        if (raw.length == 16 && isIpv4Mapped(raw)) {
            byte[] ipv4 = new byte[]{raw[12], raw[13], raw[14], raw[15]};
            try {
                InetAddress ipv4Addr = InetAddress.getByAddress(ipv4);
                return buildIpv4Arpa(ipv4Addr.getAddress());
            } catch (UnknownHostException ex) {
                // Unreachable — 4-byte array is always valid.
                return buildIpv4Arpa(ipv4);
            }
        }

        if (raw.length == 4 || addr instanceof Inet4Address) {
            return buildIpv4Arpa(raw);
        }

        // 16-byte IPv6
        return buildIpv6Arpa(raw);
    }

    private static boolean isIpv4Mapped(byte[] raw) {
        // First 10 bytes must be 0x00, bytes 10-11 must be 0xff.
        for (int i = 0; i < 10; i++) {
            if (raw[i] != 0) {
                return false;
            }
        }
        return raw[10] == (byte) 0xff && raw[11] == (byte) 0xff;
    }

    private static String buildIpv4Arpa(byte[] raw) {
        // Reverse the four octets: d.c.b.a.in-addr.arpa
        return (raw[3] & 0xff) + "." +
               (raw[2] & 0xff) + "." +
               (raw[1] & 0xff) + "." +
               (raw[0] & 0xff) + ".in-addr.arpa";
    }

    private static String buildIpv6Arpa(byte[] raw) {
        // Each byte produces two hex nibbles; reverse all 32 nibbles.
        StringBuilder sb = new StringBuilder(64);
        for (int i = 15; i >= 0; i--) {
            int octet = raw[i] & 0xff;
            sb.append(Character.forDigit(octet & 0x0f, 16));
            sb.append('.');
            sb.append(Character.forDigit((octet >> 4) & 0x0f, 16));
            sb.append('.');
        }
        sb.append("ip6.arpa");
        return sb.toString();
    }

    /**
     * Extracts the first PTR answer from a DNS response.
     * Returns {@link Optional#empty()} when no PTR answer is present.
     */
    private static Optional<String> extractPtrName(DnsResponse response) {
        int count = response.count(DnsSection.ANSWER);
        for (int i = 0; i < count; i++) {
            DnsRecord record = response.recordAt(DnsSection.ANSWER, i);
            if (record.type() != DnsRecordType.PTR) {
                continue;
            }
            try {
                String name = decodePtrRecord(record);
                if (name != null && !name.isEmpty()) {
                    // Strip a single trailing dot (fully-qualified DNS name).
                    if (name.endsWith(".")) {
                        name = name.substring(0, name.length() - 1);
                    }
                    return Optional.of(name);
                }
            } catch (Exception ex) {
                LOG.debug("Failed to decode PTR record: {}", record, ex);
            }
        }
        return Optional.empty();
    }

    /**
     * Decodes a PTR record to its target hostname string.  Netty's resolver
     * returns already-decoded {@link DnsPtrRecord} objects; raw records are
     * handled as a fallback via {@link DefaultDnsRecordDecoder#decodeName}.
     */
    private static String decodePtrRecord(DnsRecord record) {
        if (record instanceof DnsPtrRecord ptrRecord) {
            return ptrRecord.hostname();
        }
        if (record instanceof DnsRawRecord rawRecord) {
            return DefaultDnsRecordDecoder.decodeName(rawRecord.content().duplicate());
        }
        return null;
    }

    /**
     * Parses a comma-separated list of {@code host[:port]} nameserver entries
     * into {@link InetSocketAddress} instances.  The default DNS port (53) is
     * used when no port is specified.
     */
    private static List<InetSocketAddress> parseNameservers(String nameservers) {
        List<InetSocketAddress> result = new ArrayList<>();
        for (String entry : nameservers.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            // Handle IPv6 literals: "[::1]:5353" or "::1" (port defaults to 53).
            if (trimmed.startsWith("[")) {
                int closeBracket = trimmed.indexOf(']');
                if (closeBracket > 0) {
                    String host = trimmed.substring(1, closeBracket);
                    int port = 53;
                    if (closeBracket + 1 < trimmed.length() && trimmed.charAt(closeBracket + 1) == ':') {
                        port = Integer.parseInt(trimmed.substring(closeBracket + 2));
                    }
                    result.add(new InetSocketAddress(host, port));
                }
            } else {
                // IPv4 or plain hostname, optionally "host:port"
                int colonCount = trimmed.length() - trimmed.replace(":", "").length();
                if (colonCount == 1) {
                    int colon = trimmed.lastIndexOf(':');
                    String host = trimmed.substring(0, colon);
                    int port = Integer.parseInt(trimmed.substring(colon + 1));
                    result.add(new InetSocketAddress(host, port));
                } else {
                    result.add(new InetSocketAddress(trimmed, 53));
                }
            }
        }
        return result;
    }
}
