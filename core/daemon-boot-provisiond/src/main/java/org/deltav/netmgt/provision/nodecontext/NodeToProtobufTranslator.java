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

import org.deltav.timeseries.proto.InterfaceContext;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ServiceContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.opennms.netmgt.model.OnmsCategory;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsMetaData;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsSnmpInterface;

/**
 * Pure function. Translates a fully-loaded {@link OnmsNode} (with its
 * interface / service / metadata associations fetched) into a
 * {@link NodeContext} protobuf record. No side effects, no Spring state,
 * no DB access, no logging — caller handles those concerns.
 *
 * <p>Must be called inside a Hibernate session / read-only transaction
 * so LAZY-fetch associations resolve without {@code LazyInitializationException}.</p>
 */
public class NodeToProtobufTranslator {

    public NodeContext translate(OnmsNode node, long updatedAtMs) {
        NodeContext.Builder b = NodeContext.newBuilder()
                .setNodeId(node.getId() != null ? node.getId() : 0)
                .setLocation(nullSafe(locationName(node)))
                .setNodeLabel(nullSafe(node.getLabel()))
                .setForeignSource(nullSafe(node.getForeignSource()))
                .setForeignId(nullSafe(node.getForeignId()))
                .setUpdatedAtMs(updatedAtMs)
                .setDeleted(false);

        if (node.getCategories() != null) {
            for (OnmsCategory cat : node.getCategories()) {
                if (cat.getName() != null) {
                    b.addCategories(cat.getName());
                }
            }
        }

        if (node.getMetaData() != null) {
            for (OnmsMetaData m : node.getMetaData()) {
                b.putMetadata(formatMetadataKey(m), nullSafe(m.getValue()));
            }
        }

        if (node.getIpInterfaces() != null) {
            for (OnmsIpInterface iface : node.getIpInterfaces()) {
                if (iface.getIpAddress() == null) {
                    continue;
                }
                String ipKey = iface.getIpAddressAsString();
                if (ipKey == null || ipKey.isEmpty()) {
                    continue;
                }
                InterfaceContext.Builder ic = InterfaceContext.newBuilder();
                if (iface.getMetaData() != null) {
                    for (OnmsMetaData m : iface.getMetaData()) {
                        ic.putMetadata(formatMetadataKey(m), nullSafe(m.getValue()));
                    }
                }
                b.putInterfaceMetadata(ipKey, ic.build());

                if (iface.getMonitoredServices() != null) {
                    for (OnmsMonitoredService svc : iface.getMonitoredServices()) {
                        if (svc.getServiceName() == null) {
                            continue;
                        }
                        String svcKey = ipKey + "/" + svc.getServiceName();
                        ServiceContext.Builder sc = ServiceContext.newBuilder();
                        if (svc.getMetaData() != null) {
                            for (OnmsMetaData m : svc.getMetaData()) {
                                sc.putMetadata(formatMetadataKey(m), nullSafe(m.getValue()));
                            }
                        }
                        b.putServiceMetadata(svcKey, sc.build());
                    }
                }
            }
        }

        if (node.getSnmpInterfaces() != null) {
            for (OnmsSnmpInterface snmp : node.getSnmpInterfaces()) {
                if (snmp.getIfIndex() == null) {
                    // ifIndex is the map key — an interface without one cannot
                    // be addressed; skip it.
                    continue;
                }
                int ifIndex = snmp.getIfIndex();
                SnmpInterfaceContext.Builder sic = SnmpInterfaceContext.newBuilder()
                        .setIfIndex(ifIndex)
                        .setIfName(nullSafe(snmp.getIfName()))
                        .setIfDescr(nullSafe(snmp.getIfDescr()))
                        .setIfAlias(nullSafe(snmp.getIfAlias()))
                        .setIfSpeed(snmp.getIfSpeed() != null ? snmp.getIfSpeed() : 0L)
                        .setIfType(snmp.getIfType() != null ? snmp.getIfType() : 0)
                        .setPhysicalAddress(nullSafe(snmp.getPhysAddr()));
                b.putSnmpInterfaceMetadata(ifIndex, sic.build());
            }
        }

        return b.build();
    }

    /**
     * Build a tombstone (deleted=true) NodeContext. Caller MUST resolve a
     * non-null nodeId before invoking — this method takes a primitive {@code int}
     * because tombstones are only emitted in response to a {@code nodeDeleted}
     * UEI whose {@code nodeid} field is already validated upstream.
     */
    public NodeContext tombstone(int nodeId, String location, long updatedAtMs) {
        return NodeContext.newBuilder()
                .setNodeId(nodeId)
                .setLocation(nullSafe(location))
                .setUpdatedAtMs(updatedAtMs)
                .setDeleted(true)
                .build();
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    private static String locationName(OnmsNode node) {
        return node.getLocation() != null ? node.getLocation().getLocationName() : null;
    }

    private static String formatMetadataKey(OnmsMetaData m) {
        return nullSafe(m.getContext()) + ":" + nullSafe(m.getKey());
    }
}
