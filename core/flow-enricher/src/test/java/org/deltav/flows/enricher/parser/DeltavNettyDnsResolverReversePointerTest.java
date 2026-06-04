/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DeltavNettyDnsResolver#reversePointer(InetAddress)}.
 *
 * <p>Tests the pure, static pointer-name construction logic only — no
 * {@code init()} call, no live DNS, no Netty event loops required.
 */
class DeltavNettyDnsResolverReversePointerTest {

    @Test
    void ipv4_10_0_0_1() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("10.0.0.1");
        assertThat(DeltavNettyDnsResolver.reversePointer(addr))
                .isEqualTo("1.0.0.10.in-addr.arpa");
    }

    @Test
    void ipv4_192_168_1_5() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("192.168.1.5");
        assertThat(DeltavNettyDnsResolver.reversePointer(addr))
                .isEqualTo("5.1.168.192.in-addr.arpa");
    }

    @Test
    void ipv4Mapped_8_8_8_8_unwrapsToInAddrArpa() throws UnknownHostException {
        // Build ::ffff:8.8.8.8 manually as a 16-byte address.
        // Bytes: [0,0,0,0,0,0,0,0,0,0,0xff,0xff,8,8,8,8]
        byte[] raw = new byte[]{
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, (byte) 0xff, (byte) 0xff,
                8, 8, 8, 8
        };
        InetAddress addr = InetAddress.getByAddress(raw);
        assertThat(DeltavNettyDnsResolver.reversePointer(addr))
                .isEqualTo("8.8.8.8.in-addr.arpa");
    }

    @Test
    void ipv6_2001_db8_1_endsWithIp6ArpaAndStartsWithNibbles() throws UnknownHostException {
        // 2001:0db8:0000:0000:0000:0000:0000:0001
        InetAddress addr = InetAddress.getByName("2001:db8::1");
        String ptr = DeltavNettyDnsResolver.reversePointer(addr);

        assertThat(ptr).endsWith(".ip6.arpa");
        // The first nibble of the reversed form is the LSN of the last byte (0x01 → "1")
        assertThat(ptr).startsWith("1.0.0.0");
    }
}
