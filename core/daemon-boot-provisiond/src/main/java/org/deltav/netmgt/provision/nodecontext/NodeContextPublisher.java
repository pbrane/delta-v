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
package org.deltav.netmgt.provision.nodecontext;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.nio.charset.StandardCharsets;

import org.deltav.timeseries.proto.NodeContext;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.model.OnmsNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Orchestrates DB read → translate → serialize → send for the deltav-node-context
 * producer. Error-isolated: every failure path logs + increments a failure
 * counter with a reason tag, and never rethrows to the caller.
 */
public class NodeContextPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(NodeContextPublisher.class);
    private static final String BINDING_NAME = "publishNodeContext-out-0";
    static final int SIZE_WARNING_THRESHOLD_BYTES = 800_000;

    private final StreamBridge streamBridge;
    private final NodeDao nodeDao;
    private final SessionUtils sessionUtils;
    private final NodeToProtobufTranslator translator;
    private final MeterRegistry meters;

    public NodeContextPublisher(StreamBridge streamBridge,
                                NodeDao nodeDao,
                                SessionUtils sessionUtils,
                                NodeToProtobufTranslator translator,
                                MeterRegistry meters) {
        this.streamBridge = streamBridge;
        this.nodeDao = nodeDao;
        this.sessionUtils = sessionUtils;
        this.translator = translator;
        this.meters = meters;
    }

    public void publishNode(int nodeId) {
        publishNode(nodeId, "change");
    }

    public void publishNode(int nodeId, String reason) {
        Timer.Sample sample = Timer.start(meters);
        String loc = "";
        try {
            // DB read + translate MUST run inside the same read-only transaction
            // so LAZY associations (categories, metadata, interfaces, services)
            // resolve instead of throwing LazyInitializationException.
            PublishPayload payload;
            try {
                payload = sessionUtils.withReadOnlyTransaction(
                        () -> readAndTranslateInSession(nodeId, reason));
            } catch (RuntimeException ex) {
                LOG.warn("Read+translate transaction failed for nodeId={} reason={}",
                        nodeId, reason, ex);
                meters.counter("deltav_node_context_records_failed_total",
                        "location", "", "reason", "db_read_error").increment();
                return;
            }
            if (payload == null) {
                return;  // Handled path: counter already incremented inside the closure.
            }
            loc = payload.location;
            sendRecord(payload.context, loc, reason);
        } finally {
            sample.stop(Timer.builder("deltav_node_context_publish_duration_seconds")
                    .tags("location", loc, "reason", reason)
                    .register(meters));
        }
    }

    private PublishPayload readAndTranslateInSession(int nodeId, String reason) {
        OnmsNode node;
        try {
            node = nodeDao.get(nodeId);
        } catch (RuntimeException ex) {
            LOG.warn("DB read failed for nodeId={} reason={}", nodeId, reason, ex);
            meters.counter("deltav_node_context_records_failed_total",
                    "location", "", "reason", "db_read_error").increment();
            return null;
        }
        if (node == null) {
            // Benign race: a node can be deleted between the time its id lands
            // on the debounce queue and the time this publisher pulls it off.
            // This is expected under normal operation (e.g. a requisition
            // re-import deletes-and-recreates a foreign node so the old id
            // becomes orphaned before we read it) and is NOT a failure. Use
            // the skipped counter so SLO/alerting on records_failed_total
            // stays clean while ops still see the soft-miss rate here.
            LOG.debug("Node {} not found (likely deleted between event and read)", nodeId);
            meters.counter("deltav_node_context_records_skipped_total",
                    "location", "", "reason", "node_not_found").increment();
            return null;
        }
        String loc = node.getLocation() != null && node.getLocation().getLocationName() != null
                ? node.getLocation().getLocationName() : "";
        try {
            NodeContext ctx = translator.translate(node, System.currentTimeMillis());
            return new PublishPayload(loc, ctx);
        } catch (RuntimeException ex) {
            LOG.warn("Translator failed for nodeId={} reason={}", nodeId, reason, ex);
            meters.counter("deltav_node_context_records_failed_total",
                    "location", loc, "reason", "translator_error").increment();
            return null;
        }
    }

    private record PublishPayload(String location, NodeContext context) {}

    public void publishTombstone(int nodeId, String location) {
        String loc = location != null ? location : "";
        Timer.Sample sample = Timer.start(meters);
        try {
            NodeContext ctx = translator.tombstone(nodeId, loc, System.currentTimeMillis());
            sendRecord(ctx, loc, "tombstone");
        } finally {
            sample.stop(Timer.builder("deltav_node_context_publish_duration_seconds")
                    .tags("location", loc, "reason", "tombstone")
                    .register(meters));
        }
    }

    public void publishRelocation(int nodeId, String oldLocation, String newLocation) {
        String oldLoc = oldLocation != null ? oldLocation : "";
        try {
            NodeContext tomb = translator.tombstone(nodeId, oldLoc, System.currentTimeMillis());
            sendRecord(tomb, oldLoc, "relocation_old_key");
        } catch (RuntimeException ex) {
            LOG.warn("Relocation tombstone build/send failed for nodeId={} oldLocation={}",
                    nodeId, oldLoc, ex);
            meters.counter("deltav_node_context_records_failed_total",
                    "location", oldLoc, "reason", "translator_error").increment();
        }
        publishNode(nodeId, "relocation_new_key");
    }

    private void sendRecord(NodeContext ctx, String loc, String reason) {
        byte[] payload;
        try {
            payload = ctx.toByteArray();
        } catch (RuntimeException ex) {
            LOG.warn("Serialization failed for nodeId={} reason={}", ctx.getNodeId(), reason, ex);
            meters.counter("deltav_node_context_records_failed_total",
                    "location", loc, "reason", "serialization_error").increment();
            return;
        }

        if (payload.length > SIZE_WARNING_THRESHOLD_BYTES) {
            LOG.warn("Oversized NodeContext: {} bytes for nodeId={} location={} (>{} byte warning threshold)",
                    payload.length, ctx.getNodeId(), loc, SIZE_WARNING_THRESHOLD_BYTES);
            meters.counter("deltav_node_context_record_size_warning_total",
                    "location", loc).increment();
        }

        DistributionSummary.builder("deltav_node_context_record_size_bytes")
                .tags("location", loc)
                .register(meters).record(payload.length);

        byte[] key = (loc + "@" + ctx.getNodeId()).getBytes(StandardCharsets.UTF_8);
        Message<byte[]> message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.KEY, key)
                .build();

        boolean sent;
        try {
            sent = streamBridge.send(BINDING_NAME, message);
        } catch (RuntimeException ex) {
            LOG.warn("streamBridge.send threw for nodeId={} reason={}", ctx.getNodeId(), reason, ex);
            meters.counter("deltav_node_context_records_failed_total",
                    "location", loc, "reason", "kafka_send_error").increment();
            return;
        }
        if (!sent) {
            LOG.warn("streamBridge.send returned false for nodeId={} reason={}", ctx.getNodeId(), reason);
            meters.counter("deltav_node_context_records_failed_total",
                    "location", loc, "reason", "kafka_send_error").increment();
            return;
        }

        meters.counter("deltav_node_context_records_published_total",
                "location", loc, "producer", "provisiond", "reason", reason).increment();
    }
}
