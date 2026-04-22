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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.function.Supplier;

import org.deltav.timeseries.proto.NodeContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;

class NodeContextPublisherTest {

    private StreamBridge streamBridge;
    private NodeDao nodeDao;
    private SessionUtils sessionUtils;
    private NodeToProtobufTranslator translator;
    private SimpleMeterRegistry meters;
    private NodeContextPublisher publisher;

    @BeforeEach
    void setUp() {
        streamBridge = mock(StreamBridge.class);
        nodeDao = mock(NodeDao.class);
        sessionUtils = mock(SessionUtils.class);
        // Explicitly select the Supplier overload to disambiguate from the Runnable overload.
        when(sessionUtils.<Object>withReadOnlyTransaction(any(Supplier.class)))
                .thenAnswer(inv -> ((Supplier<?>) inv.getArgument(0)).get());
        translator = new NodeToProtobufTranslator();
        meters = new SimpleMeterRegistry();
        publisher = new NodeContextPublisher(streamBridge, nodeDao, sessionUtils, translator, meters);
    }

    @Test
    void publishNode_happyPath_sendsOneMessageAndIncrementsChangeCounter() {
        OnmsNode node = new OnmsNode();
        node.setId(42);
        OnmsMonitoringLocation loc = new OnmsMonitoringLocation();
        loc.setLocationName("Default");
        node.setLocation(loc);
        node.setLabel("n42");
        when(nodeDao.get(42)).thenReturn(node);
        when(streamBridge.send(eq("publishNodeContext-out-0"), any(Message.class))).thenReturn(true);

        publisher.publishNode(42);

        ArgumentCaptor<Message<byte[]>> captor = ArgumentCaptor.forClass(Message.class);
        org.mockito.Mockito.verify(streamBridge).send(eq("publishNodeContext-out-0"), captor.capture());
        byte[] key = captor.getValue().getHeaders().get(org.springframework.kafka.support.KafkaHeaders.KEY, byte[].class);
        assertThat(new String(key, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("Default@42");
        try {
            NodeContext sent = NodeContext.parseFrom(captor.getValue().getPayload());
            assertThat(sent.getNodeId()).isEqualTo(42);
            assertThat(sent.getLocation()).isEqualTo("Default");
            assertThat(sent.getNodeLabel()).isEqualTo("n42");
            assertThat(sent.getDeleted()).isFalse();
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        assertThat(meters.counter("deltav_node_context_records_published_total",
                "location", "Default", "producer", "provisiond", "reason", "change").count())
                .isEqualTo(1.0);
    }

    @Test
    void publishNode_nodeNotFound_incrementsSkippedCounterNoSend() {
        when(nodeDao.get(99)).thenReturn(null);

        publisher.publishNode(99);

        org.mockito.Mockito.verify(streamBridge, org.mockito.Mockito.never())
                .send(any(String.class), any(Message.class));
        // node_not_found is a benign race, not a failure — it lives on the
        // skipped counter so records_failed_total stays clean for SLO alerts.
        assertThat(meters.counter("deltav_node_context_records_skipped_total",
                "location", "", "reason", "node_not_found").count()).isEqualTo(1.0);
        assertThat(meters.counter("deltav_node_context_records_failed_total",
                "location", "", "reason", "node_not_found").count()).isEqualTo(0.0);
    }

    @Test
    void publishNode_daoThrows_dbReadErrorCounter() {
        when(nodeDao.get(1)).thenThrow(new RuntimeException("boom"));

        publisher.publishNode(1);

        assertThat(meters.counter("deltav_node_context_records_failed_total",
                "location", "", "reason", "db_read_error").count()).isEqualTo(1.0);
    }

    @Test
    void publishNode_translatorThrows_translatorErrorCounter() {
        OnmsNode node = new OnmsNode();
        node.setId(5);
        OnmsMonitoringLocation loc = new OnmsMonitoringLocation();
        loc.setLocationName("X");
        node.setLocation(loc);
        when(nodeDao.get(5)).thenReturn(node);

        NodeContextPublisher p2 = new NodeContextPublisher(streamBridge, nodeDao, sessionUtils,
                new NodeToProtobufTranslator() {
                    @Override public NodeContext translate(OnmsNode n, long t) {
                        throw new RuntimeException("tx");
                    }
                }, meters);

        p2.publishNode(5);

        assertThat(meters.counter("deltav_node_context_records_failed_total",
                "location", "X", "reason", "translator_error").count()).isEqualTo(1.0);
    }

    @Test
    void publishNode_sendReturnsFalse_kafkaSendErrorCounter() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        OnmsMonitoringLocation loc = new OnmsMonitoringLocation();
        loc.setLocationName("Default");
        node.setLocation(loc);
        when(nodeDao.get(1)).thenReturn(node);
        when(streamBridge.send(eq("publishNodeContext-out-0"), any(Message.class))).thenReturn(false);

        publisher.publishNode(1);

        assertThat(meters.counter("deltav_node_context_records_failed_total",
                "location", "Default", "reason", "kafka_send_error").count()).isEqualTo(1.0);
    }

    @Test
    void publishNode_sendThrows_kafkaSendErrorCounter() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        OnmsMonitoringLocation loc = new OnmsMonitoringLocation();
        loc.setLocationName("Default");
        node.setLocation(loc);
        when(nodeDao.get(1)).thenReturn(node);
        when(streamBridge.send(eq("publishNodeContext-out-0"), any(Message.class)))
                .thenThrow(new RuntimeException("broker down"));

        publisher.publishNode(1);

        assertThat(meters.counter("deltav_node_context_records_failed_total",
                "location", "Default", "reason", "kafka_send_error").count()).isEqualTo(1.0);
    }

    @Test
    void publishTombstone_sendsDeletedTrueRecord() throws Exception {
        when(streamBridge.send(eq("publishNodeContext-out-0"), any(Message.class))).thenReturn(true);

        publisher.publishTombstone(42, "Default");

        ArgumentCaptor<Message<byte[]>> captor = ArgumentCaptor.forClass(Message.class);
        org.mockito.Mockito.verify(streamBridge).send(eq("publishNodeContext-out-0"), captor.capture());
        NodeContext sent = NodeContext.parseFrom(captor.getValue().getPayload());
        assertThat(sent.getNodeId()).isEqualTo(42);
        assertThat(sent.getLocation()).isEqualTo("Default");
        assertThat(sent.getDeleted()).isTrue();
        assertThat(meters.counter("deltav_node_context_records_published_total",
                "location", "Default", "producer", "provisiond", "reason", "tombstone").count())
                .isEqualTo(1.0);
    }

    @Test
    void publishRelocation_emitsTwoRecordsWithCorrectReasons() throws Exception {
        OnmsNode node = new OnmsNode();
        node.setId(7);
        OnmsMonitoringLocation loc = new OnmsMonitoringLocation();
        loc.setLocationName("Site-B");
        node.setLocation(loc);
        when(nodeDao.get(7)).thenReturn(node);
        when(streamBridge.send(eq("publishNodeContext-out-0"), any(Message.class))).thenReturn(true);

        publisher.publishRelocation(7, "Site-A", "Site-B");

        org.mockito.Mockito.verify(streamBridge, org.mockito.Mockito.times(2))
                .send(eq("publishNodeContext-out-0"), any(Message.class));
        assertThat(meters.counter("deltav_node_context_records_published_total",
                "location", "Site-A", "producer", "provisiond", "reason", "relocation_old_key").count())
                .isEqualTo(1.0);
        assertThat(meters.counter("deltav_node_context_records_published_total",
                "location", "Site-B", "producer", "provisiond", "reason", "relocation_new_key").count())
                .isEqualTo(1.0);
    }

    @Test
    void publishNode_oversizedPayload_warnsButStillSends() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        OnmsMonitoringLocation loc = new OnmsMonitoringLocation();
        loc.setLocationName("Big");
        node.setLocation(loc);
        String filler = "x".repeat(10_000);
        for (int i = 0; i < 100; i++) {
            node.getMetaData().add(new org.opennms.netmgt.model.OnmsMetaData("ctx", "k" + i, filler));
        }
        when(nodeDao.get(1)).thenReturn(node);
        when(streamBridge.send(eq("publishNodeContext-out-0"), any(Message.class))).thenReturn(true);

        publisher.publishNode(1);

        assertThat(meters.counter("deltav_node_context_record_size_warning_total",
                "location", "Big").count()).isEqualTo(1.0);
        org.mockito.Mockito.verify(streamBridge, org.mockito.Mockito.times(1))
                .send(eq("publishNodeContext-out-0"), any(Message.class));
    }
}
