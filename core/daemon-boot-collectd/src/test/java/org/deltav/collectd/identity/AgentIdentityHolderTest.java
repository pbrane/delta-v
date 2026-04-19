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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class AgentIdentityHolderTest {

    @Test
    void setThenGetOrThrowReturnsIdentity() {
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(42, "Site-A");

        AgentIdentity id = holder.getOrThrow();
        assertThat(id.nodeId()).isEqualTo(42);
        assertThat(id.location()).isEqualTo("Site-A");
    }

    @Test
    void getOrThrowOnEmptyHolderThrowsIllegalStateException() {
        AgentIdentityHolder holder = new AgentIdentityHolder();

        assertThatThrownBy(holder::getOrThrow)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("AgentIdentity not populated");
    }

    @Test
    void clearRemovesPreviousValue() {
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(1, "X");
        holder.clear();

        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void clearIsIdempotent() {
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.clear();      // no prior set — must not throw
        holder.clear();      // second call — must not throw
    }

    @Test
    void setWithNullLocationNormalizesToEmpty() {
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(3, null);
        assertThat(holder.getOrThrow().location()).isEqualTo("");
    }

    @Test
    void doubleSetOverwritesSilently() {
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(1, "First");
        holder.set(2, "Second");
        AgentIdentity id = holder.getOrThrow();
        assertThat(id.nodeId()).isEqualTo(2);
        assertThat(id.location()).isEqualTo("Second");
    }

    @Test
    void threadIsolationOtherThreadCannotSeeValue() throws InterruptedException {
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(99, "MainThread");

        AtomicReference<Throwable> otherThreadError = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                holder.getOrThrow();
            } catch (Throwable e) {
                otherThreadError.set(e);
            }
        });
        t.start();
        t.join();

        assertThat(otherThreadError.get()).isInstanceOf(IllegalStateException.class);
        // Main thread's value still intact:
        assertThat(holder.getOrThrow().nodeId()).isEqualTo(99);
    }
}
