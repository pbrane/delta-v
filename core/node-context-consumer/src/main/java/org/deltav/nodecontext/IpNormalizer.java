/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Canonicalizes IP strings to the same form provisiond uses for
 * NodeContext.interface_metadata keys: OnmsIpInterface.getIpAddressAsString()
 * -> java.net.InetAddress.getHostAddress() (IPv4 dotted-decimal without leading
 * zeros, compressed IPv6). All getByIp lookups and index keys MUST pass here.
 */
public final class IpNormalizer {
    private IpNormalizer() {}

    /** @return canonical host-address form, or null if null/blank/unparseable. */
    public static String normalize(String ip) {
        if (ip == null || ip.isBlank()) return null;
        try {
            return InetAddress.getByName(ip.trim()).getHostAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
