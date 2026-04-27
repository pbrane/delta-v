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

import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.deltav.minion.grpc.v1.RpcRequest;
import org.deltav.minion.grpc.v1.RpcResponse;
import org.opennms.core.ipc.rpc.kafka.model.RpcMessageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The "dispatcher" piece of Decision 1 Option D. Implements both
 * {@link RpcStreamCloseHandler} (redispatch on stream close) and
 * {@link RpcResponseHandler} (forward Minion responses to internal Kafka).
 *
 * <p>Inbound RPCs from ServiceDaemons arrive on
 * {@code OpenNMS.<location>.rpc-request} (the topic name is parameterized
 * but matches horizon's KafkaRpcClient producer convention). The dispatcher
 * picks a stream from the location's pool, records the in-flight entry,
 * and forwards.
 *
 * <p>If no stream is available for a location (empty pool), the request
 * is dropped. Per Decision 1 sub-decision 1-i, the daemon's existing
 * timeout path absorbs this case (no synthesized outage per
 * {@code feedback_rpc_timeout_no_outages}).
 */
@Component
public class RpcChannelDispatcher implements RpcStreamCloseHandler, RpcResponseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RpcChannelDispatcher.class);

    private final MinionStreamPool pool;
    private final InFlightRpcTable inFlight;
    private final RpcResponsePublisher publisher;

    public RpcChannelDispatcher(MinionStreamPool pool,
                                InFlightRpcTable inFlight,
                                RpcResponsePublisher publisher) {
        this.pool = pool;
        this.inFlight = inFlight;
        this.publisher = publisher;
    }

    /**
     * Topic name encodes the Minion location: {@code OpenNMS.<location>.rpc-request}.
     * Horizon's RpcMessageProto does NOT carry a location field — that information
     * lives only in the Kafka topic name on horizon's side. The translator below
     * extracts location from the topic name and populates rc2's RpcRequest accordingly.
     */
    private static final Pattern RPC_REQUEST_TOPIC = Pattern.compile("OpenNMS\\.(.*)\\.rpc-request");

    @KafkaListener(
        topicPattern = "OpenNMS\\..*\\.rpc-request",
        groupId = "minion-gateway-rpc",
        containerFactory = "rpcRequestContainerFactory"
    )
    public void onKafkaRequest(ConsumerRecord<String, byte[]> record) {
        try {
            // Daemon-side wire format on this topic is horizon's RpcMessageProto, NOT
            // rc2's RpcRequest. Translate at the boundary; the rc2 proto is what flows
            // over the gRPC stream to the Minion (where MinionRpcStreamClient unmarshals
            // payload bytes via RpcModule.unmarshalRequest).
            RpcMessageProto horizonMsg = RpcMessageProto.parseFrom(record.value());
            String location = extractLocationFromTopic(record.topic());
            if (location == null) {
                LOG.warn("Could not extract location from topic={}; dropping rpcId={}",
                    record.topic(), horizonMsg.getRpcId());
                return;
            }
            Instant now = Instant.now();
            RpcRequest req = RpcRequest.newBuilder()
                .setRpcId(horizonMsg.getRpcId())
                .setModuleId(horizonMsg.getModuleId())
                .setMinionId(horizonMsg.getSystemId())  // may be empty; Minion reads identity from gRPC metadata
                .setLocation(location)
                .setPayload(horizonMsg.getRpcContent())
                .setDeadlineMs(horizonMsg.getExpirationTime())
                .setDispatchedAt(Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build())
                .build();
            dispatch(req);
        } catch (Exception e) {
            LOG.warn("Failed to parse RpcMessageProto from topic={} key={}", record.topic(), record.key(), e);
        }
    }

    static String extractLocationFromTopic(String topic) {
        if (topic == null) {
            return null;
        }
        Matcher m = RPC_REQUEST_TOPIC.matcher(topic);
        return m.matches() ? m.group(1) : null;
    }

    void dispatch(RpcRequest req) {
        StreamObserver<RpcRequest> stream = pool.pickStream(req.getLocation());
        if (stream == null) {
            LOG.info("No Minion streams for location={}; dropping rpcId={} (caller will time out)",
                req.getLocation(), req.getRpcId());
            return;
        }

        Instant deadline = req.getDeadlineMs() > 0
            ? Instant.ofEpochMilli(req.getDeadlineMs())
            : Instant.now().plusSeconds(60);
        inFlight.record(req.getRpcId(), stream, req, deadline);
        try {
            stream.onNext(req);
        } catch (Throwable t) {
            LOG.warn("Stream send failed for rpcId={}; evicting in-flight entry", req.getRpcId(), t);
            inFlight.complete(req.getRpcId());
        }
    }

    /**
     * Redispatches in-flight RPCs from a closed stream to a surviving sibling
     * in the same location's pool. Per Decision 1 sub-decision 1-iii of the
     * v1.2.0-rc2 decisions doc.
     *
     * <p>Race: between {@code evictByStream} returning orphans and
     * {@code sibling.onNext()} firing on each orphan, the chosen sibling
     * stream may itself close. In that case {@code sibling.onNext()} throws,
     * the in-flight entry is removed, and the RPC is silently lost. This is
     * acceptable per {@code feedback_rpc_timeout_no_outages}: the caller's
     * existing dispatcher deadline absorbs the timeout. We do NOT attempt a
     * second redispatch round (a "retry on a different sibling" loop) because
     * the at-least-once execution contract from Decision 1 sub-decision 1-ii
     * already permits the caller to retry, and unbounded redispatch loops add
     * complexity without changing observable behavior.
     */
    @Override
    public void onStreamClosed(String location, StreamObserver<RpcRequest> closedStream) {
        List<InFlightRpcTable.Entry> orphans = inFlight.evictByStream(closedStream);
        if (orphans.isEmpty()) {
            return;
        }
        List<StreamObserver<RpcRequest>> siblings = pool.siblingStreams(location, closedStream);
        if (siblings.isEmpty()) {
            LOG.info("Stream closed for location={} with {} in-flight RPCs; no siblings; caller will time out",
                location, orphans.size());
            return;
        }
        // Round-robin across siblings; redispatch each orphan
        int n = siblings.size();
        for (int i = 0; i < orphans.size(); i++) {
            InFlightRpcTable.Entry orphan = orphans.get(i);
            StreamObserver<RpcRequest> sibling = siblings.get(i % n);
            inFlight.record(orphan.rpcId(), sibling, orphan.request(), orphan.deadline());
            try {
                sibling.onNext(orphan.request());
            } catch (Throwable t) {
                LOG.warn("Redispatch send failed for rpcId={}", orphan.rpcId(), t);
                inFlight.complete(orphan.rpcId());
            }
        }
        LOG.info("Redispatched {} in-flight RPCs from closed stream for location={}", orphans.size(), location);
    }

    @Override
    public void handle(RpcResponse response) {
        inFlight.complete(response.getRpcId());
        publisher.publish(response);
    }

    /**
     * Periodically sweeps in-flight RPC entries past their deadline. Without
     * this, Minion hangs (server-side processing wedged with no stream close)
     * would leak entries indefinitely.
     */
    @org.springframework.scheduling.annotation.Scheduled(fixedDelay = 30000L)
    public void sweepExpired() {
        java.util.List<InFlightRpcTable.Entry> expired = inFlight.evictExpired(java.time.Instant.now());
        if (!expired.isEmpty()) {
            LOG.info("Swept {} expired in-flight RPCs (deadline-based)", expired.size());
        }
    }
}
