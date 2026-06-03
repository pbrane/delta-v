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
package org.deltav.flows.enricher;

import org.deltav.nodecontext.NodeContextCacheReadyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Holds the flow ingest binding ({@code enrichFlows-in-0}, auto-startup=false)
 * until the NodeContext cache has drained to the Kafka HWM, so the first flow
 * processed sees the full node/interface inventory.
 */
@Component
public class NodeContextReadinessGate {

    private static final Logger LOG = LoggerFactory.getLogger(NodeContextReadinessGate.class);
    private static final String INPUT_BINDING = "enrichFlows-in-0";

    private final BindingsLifecycleController bindings;

    public NodeContextReadinessGate(BindingsLifecycleController bindings) {
        this.bindings = bindings;
    }

    @EventListener
    public void onReady(NodeContextCacheReadyEvent event) {
        LOG.info("NodeContext cache ready — starting flow ingest binding {}", INPUT_BINDING);
        bindings.changeState(INPUT_BINDING, State.STARTED);
    }
}
