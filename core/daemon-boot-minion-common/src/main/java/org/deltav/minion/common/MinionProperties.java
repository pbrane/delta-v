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
package org.deltav.minion.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "opennms.minion")
public class MinionProperties {

    private String id = "00000000-0000-0000-0000-000000000001";
    private String location = "Default";

    /**
     * Heartbeat transport selection. {@code grpc} (default) routes Heartbeat
     * via {@link org.deltav.minion.common.grpc.GrpcMessageDispatcherFactory};
     * {@code kafka} aliases the primary Kafka factory under the same bean
     * name (rollback path). Other sinks remain on Kafka in rc1 regardless.
     */
    private String transport = "grpc";

    private final Gateway gateway = new Gateway();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getTransport() {
        return transport;
    }

    public void setTransport(String transport) {
        this.transport = transport;
    }

    public Gateway getGateway() {
        return gateway;
    }

    public static class Gateway {
        private String host = "envoy";
        private int port = 8443;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }
}
