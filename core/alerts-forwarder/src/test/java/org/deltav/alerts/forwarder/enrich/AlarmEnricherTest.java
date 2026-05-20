/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.enrich;

import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.nodecontext.NodeContextCache;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AlarmEnricherTest {

    @Test
    void enrichesWithNodeIdentityAndDimensions() {
        NodeContextCache cache = new NodeContextCache();
        cache.put("Default@1042", NodeContext.newBuilder()
                .setNodeId(1042).setLocation("Default").setNodeLabel("web01.corp")
                .setForeignSource("Servers").setForeignId("web-01")
                .addCategories("Production").build());
        AlarmEnricher enricher = new AlarmEnricher(cache);

        AlarmState alarm = AlarmState.newBuilder()
                .setReductionKey("rk").setAlarmId(7).setNodeId(1042).setLocation("Default")
                .setSeverity(AlarmState.Severity.MAJOR).setUei("uei/nodeDown")
                .setDescription("Node is down").setLogMessage("down since 10:00").build();

        EnrichedAlarm e = enricher.enrich(alarm);

        assertThat(e.enrichmentComplete()).isTrue();
        assertThat(e.labels()).containsEntry("node", "Servers:web-01");
        assertThat(e.labels()).containsEntry("severity", "MAJOR");
        assertThat(e.labels()).containsEntry("uei", "uei/nodeDown");
        assertThat(e.labels()).containsEntry("foreign_source", "Servers");
        assertThat(e.labels()).containsEntry("categories", "Production");
        assertThat(e.labels()).containsEntry("alarm_id", "7");
        assertThat(e.annotations()).containsEntry("description", "Node is down");
        assertThat(e.annotations()).containsEntry("log_message", "down since 10:00");
    }

    @Test
    void addsSnmpInterfaceLabelsWhenIfIndexPresent() {
        NodeContextCache cache = new NodeContextCache();
        cache.put("Default@1042", NodeContext.newBuilder()
                .setNodeId(1042).setLocation("Default")
                .setForeignSource("Servers").setForeignId("web-01")
                .putSnmpInterfaceMetadata(3, SnmpInterfaceContext.newBuilder()
                        .setIfIndex(3).setIfName("Gi0/3").setIfDescr("GigabitEthernet0/3")
                        .setIfAlias("uplink").setIfSpeed(1_000_000_000L).setIfType(6).build())
                .build());
        AlarmEnricher enricher = new AlarmEnricher(cache);

        AlarmState alarm = AlarmState.newBuilder()
                .setReductionKey("rk").setNodeId(1042).setLocation("Default")
                .setSeverity(AlarmState.Severity.MINOR).setUei("uei/ifDown").setIfIndex(3).build();

        EnrichedAlarm e = enricher.enrich(alarm);

        assertThat(e.labels()).containsEntry("if_name", "Gi0/3");
        assertThat(e.labels()).containsEntry("if_alias", "uplink");
        assertThat(e.labels()).containsEntry("if_speed", "1000000000");
    }

    @Test
    void partialEnrichmentWhenNodeMissing() {
        AlarmEnricher enricher = new AlarmEnricher(new NodeContextCache());
        AlarmState alarm = AlarmState.newBuilder()
                .setReductionKey("rk").setNodeId(999).setLocation("Default")
                .setSeverity(AlarmState.Severity.MAJOR).setUei("uei/x").build();

        EnrichedAlarm e = enricher.enrich(alarm);

        assertThat(e.enrichmentComplete()).isFalse();
        assertThat(e.labels()).containsEntry("node", "delta-v:999");
        assertThat(e.labels()).containsEntry("enrichment", "partial");
    }
}
