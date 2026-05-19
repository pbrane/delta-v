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

import java.net.InetAddress;
import java.util.List;

import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.model.OnmsCategory;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsMetaData;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsServiceType;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;

class NodeToProtobufTranslatorTest {

    private final NodeToProtobufTranslator translator = new NodeToProtobufTranslator();

    @Test
    void tombstone_returnsMinimalDeletedRecord() {
        NodeContext tombstone = translator.tombstone(42, "Default", 1_700_000_000L);

        assertThat(tombstone.getNodeId()).isEqualTo(42);
        assertThat(tombstone.getLocation()).isEqualTo("Default");
        assertThat(tombstone.getDeleted()).isTrue();
        assertThat(tombstone.getUpdatedAtMs()).isEqualTo(1_700_000_000L);
        assertThat(tombstone.getNodeLabel()).isEmpty();
        assertThat(tombstone.getForeignSource()).isEmpty();
        assertThat(tombstone.getForeignId()).isEmpty();
        assertThat(tombstone.getCategoriesCount()).isZero();
        assertThat(tombstone.getMetadataCount()).isZero();
    }

    @Test
    void translate_emptyNode_populatesIdLocationAndTimestamp() {
        OnmsNode node = new OnmsNode();
        node.setId(10);
        node.setLocation(location("Default"));

        NodeContext ctx = translator.translate(node, 1_700L);

        assertThat(ctx.getNodeId()).isEqualTo(10);
        assertThat(ctx.getLocation()).isEqualTo("Default");
        assertThat(ctx.getUpdatedAtMs()).isEqualTo(1_700L);
        assertThat(ctx.getDeleted()).isFalse();
        assertThat(ctx.getNodeLabel()).isEmpty();
        assertThat(ctx.getCategoriesCount()).isZero();
        assertThat(ctx.getMetadataCount()).isZero();
        assertThat(ctx.getInterfaceMetadataCount()).isZero();
        assertThat(ctx.getServiceMetadataCount()).isZero();
    }

    @Test
    void translate_fullNode_populatesAllFields() {
        OnmsNode node = new OnmsNode();
        node.setId(7);
        node.setLocation(location("Site-A"));
        node.setLabel("host-7.example.com");
        node.setForeignSource("provision-prod");
        node.setForeignId("host-7");
        node.getCategories().add(category("prod"));
        node.getCategories().add(category("linux"));
        node.getMetaData().add(new OnmsMetaData("requisition", "sysLocation", "rack-42"));
        node.getMetaData().add(new OnmsMetaData("snmp", "sysContact", "ops@example.com"));

        NodeContext ctx = translator.translate(node, 1_000L);

        assertThat(ctx.getNodeLabel()).isEqualTo("host-7.example.com");
        assertThat(ctx.getForeignSource()).isEqualTo("provision-prod");
        assertThat(ctx.getForeignId()).isEqualTo("host-7");
        assertThat(ctx.getCategoriesList()).containsExactlyInAnyOrder("prod", "linux");
        assertThat(ctx.getMetadataMap())
                .containsEntry("requisition:sysLocation", "rack-42")
                .containsEntry("snmp:sysContact", "ops@example.com");
    }

    @Test
    void translate_nullLabel_rendersEmptyString() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        node.setLabel(null);

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getNodeLabel()).isEmpty();
    }

    @Test
    void translate_nullLocation_rendersEmptyString() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(null);

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getLocation()).isEmpty();
    }

    @Test
    void translate_nullCategorySet_emitsNoCategories() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        node.setCategories(null);

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getCategoriesCount()).isZero();
    }

    @Test
    void translate_metadataKeys_formattedAsContextColonKey() throws Exception {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        node.getMetaData().add(new OnmsMetaData("requisition", "alpha", "A"));
        node.getMetaData().add(new OnmsMetaData("snmp", "beta", "B"));
        node.getMetaData().add(new OnmsMetaData("ip", "hostname", "h"));

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getMetadataMap())
                .containsEntry("requisition:alpha", "A")
                .containsEntry("snmp:beta", "B")
                .containsEntry("ip:hostname", "h");
    }

    @Test
    void translate_ipv4Interface_keyedByIpString() throws Exception {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        OnmsIpInterface ip = new OnmsIpInterface();
        ip.setIpAddress(InetAddress.getByName("192.0.2.5"));
        ip.getMetaData().add(new OnmsMetaData("iface", "alias", "uplink"));
        node.addIpInterface(ip);

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getInterfaceMetadataMap()).containsKey("192.0.2.5");
        assertThat(ctx.getInterfaceMetadataMap().get("192.0.2.5").getMetadataMap())
                .containsEntry("iface:alias", "uplink");
    }

    @Test
    void translate_ipv6Interface_keyedByCanonicalIpString() throws Exception {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        OnmsIpInterface ip = new OnmsIpInterface();
        ip.setIpAddress(InetAddress.getByName("2001:db8::1"));
        node.addIpInterface(ip);

        NodeContext ctx = translator.translate(node, 0L);

        // Pin the test to the same key the production code emits: whatever
        // OnmsIpInterface.getIpAddressAsString() returns is the wire-level key
        // downstream consumers must match. Re-deriving here rather than
        // hard-coding a literal makes the test robust against JDK-version
        // changes in InetAddress.getHostAddress().
        String expectedKey = ip.getIpAddressAsString();
        assertThat(ctx.getInterfaceMetadataMap()).containsOnlyKeys(expectedKey);
        assertThat(expectedKey).contains(":");
    }

    @Test
    void translate_serviceMetadata_keyedByIpSlashServiceName() throws Exception {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        OnmsIpInterface ip = new OnmsIpInterface();
        ip.setIpAddress(InetAddress.getByName("192.0.2.1"));

        OnmsServiceType icmpType = new OnmsServiceType("ICMP");
        OnmsMonitoredService svc = new OnmsMonitoredService(ip, icmpType);
        svc.getMetaData().add(new OnmsMetaData("svc", "criticality", "high"));
        ip.addMonitoredService(svc);
        node.addIpInterface(ip);

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getServiceMetadataMap()).containsKey("192.0.2.1/ICMP");
        assertThat(ctx.getServiceMetadataMap().get("192.0.2.1/ICMP").getMetadataMap())
                .containsEntry("svc:criticality", "high");
    }

    @Test
    void translate_unicodeLabelAndMetadata_roundTripsUtf8() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        node.setLabel("server-\u00e9-\u4e2d\u6587");
        node.getMetaData().add(new OnmsMetaData("meta", "note", "\u00e9 \u4e2d\u6587"));

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getNodeLabel()).isEqualTo("server-\u00e9-\u4e2d\u6587");
        assertThat(ctx.getMetadataMap()).containsEntry("meta:note", "\u00e9 \u4e2d\u6587");
    }

    @Test
    void translate_nullIdFallbackToZero() {
        OnmsNode node = new OnmsNode();
        node.setId(null);
        node.setLocation(location("Default"));

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getNodeId()).isZero();
    }

    @Test
    void translate_nullInterfaceIp_skipsInterface() throws Exception {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        OnmsIpInterface ip = new OnmsIpInterface();
        ip.setIpAddress(null);
        node.addIpInterface(ip);

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getInterfaceMetadataCount()).isZero();
        assertThat(ctx.getServiceMetadataCount()).isZero();
    }

    @Test
    void translate_largeNode_serializesSuccessfully() throws Exception {
        OnmsNode node = new OnmsNode();
        node.setId(99);
        node.setLocation(location("Default"));
        for (int i = 0; i < 50; i++) {
            node.getMetaData().add(new OnmsMetaData("ctx", "key-" + i, "value-" + i));
        }
        for (int i = 0; i < 100; i++) {
            OnmsIpInterface ip = new OnmsIpInterface();
            ip.setIpAddress(InetAddress.getByName("10.0." + (i / 256) + "." + (i % 256)));
            node.addIpInterface(ip);
        }

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.toByteArray()).isNotEmpty();
        assertThat(ctx.getInterfaceMetadataCount()).isEqualTo(100);
        assertThat(ctx.getMetadataCount()).isEqualTo(50);
    }

    @Test
    void translate_snmpInterface_keyedByIfIndex() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));

        // The 2-arg OnmsSnmpInterface constructor adds itself to node.getSnmpInterfaces().
        OnmsSnmpInterface snmp = new OnmsSnmpInterface(node, 3);
        snmp.setIfName("Gi0/3");
        snmp.setIfDescr("GigabitEthernet0/3");
        snmp.setIfAlias("uplink-to-core");
        snmp.setIfSpeed(1_000_000_000L);
        snmp.setIfType(6);
        snmp.setPhysAddr("00:11:22:33:44:55");

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getSnmpInterfaceMetadataMap()).containsKey(3);
        SnmpInterfaceContext sic = ctx.getSnmpInterfaceMetadataMap().get(3);
        assertThat(sic.getIfIndex()).isEqualTo(3);
        assertThat(sic.getIfName()).isEqualTo("Gi0/3");
        assertThat(sic.getIfDescr()).isEqualTo("GigabitEthernet0/3");
        assertThat(sic.getIfAlias()).isEqualTo("uplink-to-core");
        assertThat(sic.getIfSpeed()).isEqualTo(1_000_000_000L);
        assertThat(sic.getIfType()).isEqualTo(6);
        assertThat(sic.getPhysicalAddress()).isEqualTo("00:11:22:33:44:55");
    }

    @Test
    void translate_snmpInterface_withNullIfIndex_isSkipped() {
        OnmsNode node = new OnmsNode();
        node.setId(1);
        node.setLocation(location("Default"));
        OnmsSnmpInterface snmp = new OnmsSnmpInterface();
        snmp.setNode(node);
        snmp.setIfName("no-index");          // ifIndex left null
        node.getSnmpInterfaces().add(snmp);

        NodeContext ctx = translator.translate(node, 0L);

        assertThat(ctx.getSnmpInterfaceMetadataMap()).isEmpty();
    }

    // --- helpers ---

    private static OnmsMonitoringLocation location(String name) {
        OnmsMonitoringLocation loc = new OnmsMonitoringLocation();
        loc.setLocationName(name);
        return loc;
    }

    private static OnmsCategory category(String name) {
        OnmsCategory c = new OnmsCategory();
        c.setName(name);
        return c;
    }
}
