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
package org.deltav.gateway.rpc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deltav.minion.grpc.v1.RpcResponse;
import org.opennms.core.ipc.rpc.kafka.model.RpcMessageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Publishes RPC responses to {@code DeltaV.rpc-response} in horizon's
 * RpcMessageProto wire format. Translates from rc2's RpcResponse (used on
 * the gateway-Minion gRPC stream) so horizon's KafkaRpcClient on the daemon
 * side can correlate by rpc_id and unmarshal rpc_content as before.
 */
@Component
public class RpcResponsePublisher {

    private static final Logger LOG = LoggerFactory.getLogger(RpcResponsePublisher.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String responseTopic;

    public RpcResponsePublisher(KafkaProducer<String, byte[]> minionGatewayKafkaProducer,
                                @Value("${minion-gateway.rpc.response-topic:DeltaV.rpc-response}") String responseTopic) {
        this.producer = minionGatewayKafkaProducer;
        this.responseTopic = responseTopic;
    }

    public void publish(RpcResponse response) {
        // KafkaRpcClient on the daemon side correlates by rpc_id only and unmarshals
        // rpc_content via RpcModule.unmarshalResponse. The Minion's MinionRpcStreamClient
        // already produced marshaled response bytes via RpcModule.marshalResponse(rsp);
        // on module-execution failure the same client wraps the exception via
        // createResponseWithException then marshals. Either way, rc2 RpcResponse.payload
        // is the marshaled-response byte stream that horizon's RpcMessageProto.rpc_content
        // expects.
        //
        // Single-chunk responses only in PR1. Large responses requiring chunking is
        // tracked as a follow-up post-PR1.
        RpcMessageProto horizonMsg = RpcMessageProto.newBuilder()
            .setRpcId(response.getRpcId())
            .setRpcContent(response.getPayload())
            .setCurrentChunkNumber(0)
            .setTotalChunks(1)
            .build();
        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(responseTopic, response.getRpcId(), horizonMsg.toByteArray());
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                LOG.warn("Failed to publish RPC response rpcId={}", response.getRpcId(), ex);
            }
        });
    }
}
