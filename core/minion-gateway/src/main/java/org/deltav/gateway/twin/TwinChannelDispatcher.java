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
package org.deltav.gateway.twin;

import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.springframework.stereotype.Component;

/**
 * Phase 2 placeholder for the Twin dispatcher. Task 8 replaces this stub
 * with the full Kafka @KafkaListener + state cache + broadcast implementation.
 * Defined at the Task 7 boundary so {@link org.deltav.gateway.grpc.TwinChannelGrpcService}
 * can compile its constructor parameter type.
 */
@Component
public class TwinChannelDispatcher {

    public void sendInitialSnapshot(String consumerKey, String location, StreamObserver<TwinUpdate> observer) {
        // Task 8 implements full snapshot read-through from TwinStateCache.
    }
}
