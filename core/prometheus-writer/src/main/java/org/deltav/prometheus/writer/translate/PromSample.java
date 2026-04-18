/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import java.util.Map;

public record PromSample(String name, Map<String, String> labels, double value, long timestampMs) {}
