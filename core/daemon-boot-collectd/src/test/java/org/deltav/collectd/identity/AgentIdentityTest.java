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
package org.deltav.collectd.identity;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AgentIdentityTest {

    @Test
    void nullLocationNormalizesToEmptyString() {
        AgentIdentity id = new AgentIdentity(42, null);
        assertThat(id.nodeId()).isEqualTo(42);
        assertThat(id.location()).isEqualTo("");
    }

    @Test
    void nonNullLocationPassesThrough() {
        AgentIdentity id = new AgentIdentity(7, "Site-A");
        assertThat(id.location()).isEqualTo("Site-A");
    }

    @Test
    void emptyStringLocationPreserved() {
        AgentIdentity id = new AgentIdentity(1, "");
        assertThat(id.location()).isEqualTo("");
    }

    @Test
    void zeroNodeIdAccepted() {
        // No validation at record construction — persist-time check handles it.
        AgentIdentity id = new AgentIdentity(0, "Default");
        assertThat(id.nodeId()).isZero();
    }

    @Test
    void negativeNodeIdAccepted() {
        AgentIdentity id = new AgentIdentity(-5, "Default");
        assertThat(id.nodeId()).isEqualTo(-5);
    }

    @Test
    void recordEqualityOnBothFields() {
        assertThat(new AgentIdentity(3, "X")).isEqualTo(new AgentIdentity(3, "X"));
        assertThat(new AgentIdentity(3, "X")).isNotEqualTo(new AgentIdentity(4, "X"));
        assertThat(new AgentIdentity(3, "X")).isNotEqualTo(new AgentIdentity(3, "Y"));
    }
}
