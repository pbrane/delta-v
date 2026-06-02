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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.deltav.nodecontext.NodeContextCacheReadyEvent;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.cloud.stream.binding.BindingsLifecycleController.State;

class NodeContextReadinessGateTest {

    @Test
    void startsInputBindingWhenCacheBecomesReady() {
        BindingsLifecycleController controller = mock(BindingsLifecycleController.class);
        NodeContextReadinessGate gate = new NodeContextReadinessGate(controller);

        gate.onReady(new NodeContextCacheReadyEvent(this, 42L, 100));

        verify(controller).changeState("enrichFlows-in-0", State.STARTED);
    }
}
