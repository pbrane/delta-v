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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Publishes RpcResponse protobuf bytes to {@code OpenNMS.rpc-response}
 * (the topic horizon's KafkaRpcClient consumes for response correlation).
 *
 * <p>The wire format (RpcResponseProto from horizon's RPC API) matches
 * what KafkaRpcClient expects, so daemon-side correlation continues to
 * work unchanged.
 */
@Component
public class RpcResponsePublisher {

    private static final Logger LOG = LoggerFactory.getLogger(RpcResponsePublisher.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String responseTopic;

    public RpcResponsePublisher(KafkaProducer<String, byte[]> minionGatewayKafkaProducer,
                                @Value("${minion-gateway.rpc.response-topic:OpenNMS.rpc-response}") String responseTopic) {
        this.producer = minionGatewayKafkaProducer;
        this.responseTopic = responseTopic;
    }

    public void publish(RpcResponse response) {
        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(responseTopic, response.getRpcId(), response.toByteArray());
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                LOG.warn("Failed to publish RPC response rpcId={}", response.getRpcId(), ex);
            }
        });
    }
}
