/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.enrich;

import java.util.Optional;

import org.deltav.timeseries.proto.NodeContext;

/**
 * Durable node identity (spec §4): {@code foreignSource:foreignId} for a
 * provisioned node, else {@code delta-v:{node_id}} for a discovery node with
 * no requisition. Identical to prometheus-writer's
 * {@code InstanceLabelResolver.nodeIdentity} — duplicated because the two
 * modules must not depend on each other.
 */
public final class NodeIdentity {
    private NodeIdentity() {}

    public static String of(int nodeId, Optional<NodeContext> nc) {
        String fs = nc.map(NodeContext::getForeignSource).orElse("");
        String fi = nc.map(NodeContext::getForeignId).orElse("");
        if (!fs.isEmpty() && !fi.isEmpty()) {
            return fs + ":" + fi;
        }
        return "delta-v:" + nodeId;
    }
}
