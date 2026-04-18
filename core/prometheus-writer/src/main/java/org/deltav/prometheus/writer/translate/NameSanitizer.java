/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Component
public class NameSanitizer {
    private static final Logger LOG = LoggerFactory.getLogger(NameSanitizer.class);

    public String sanitize(String input) {
        if (input == null || input.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if ((c >= 'A' && c <= 'Z')) sb.append((char)(c + ('a' - 'A')));
            else if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) sb.append(c);
            else sb.append('_');
        }
        String collapsed = sb.toString().replaceAll("_+", "_");
        if (collapsed.isEmpty()) return "";
        if (Character.isDigit(collapsed.charAt(0))) collapsed = "_" + collapsed;
        return collapsed;
    }

    /** Detects collisions in a name set; returns map of sanitized &rarr; list of originals where &gt; 1. */
    public Map<String, List<String>> detectCollisions(List<String> originals) {
        Map<String, List<String>> buckets = new TreeMap<>();
        for (String orig : originals) {
            String clean = sanitize(orig);
            buckets.computeIfAbsent(clean, k -> new java.util.ArrayList<>()).add(orig);
        }
        Map<String, List<String>> collisions = new HashMap<>();
        for (Map.Entry<String, List<String>> e : buckets.entrySet()) {
            if (e.getValue().size() > 1) {
                collisions.put(e.getKey(), e.getValue());
                LOG.warn("Name sanitization collision: {} -> {}", e.getValue(), e.getKey());
            }
        }
        return collisions;
    }
}
