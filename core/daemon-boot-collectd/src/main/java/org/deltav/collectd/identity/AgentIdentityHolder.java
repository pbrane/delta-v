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

/**
 * ThreadLocal-backed holder for {@link AgentIdentity} captured by
 * {@code AgentIdentityCapturingCollectorClient} before RPC dispatch and
 * consumed by {@code TimeseriesKafkaPersister} at publish time.
 *
 * <p>Contract invariants:</p>
 * <ul>
 *   <li>{@link #set(int, String)} is unconditional overwrite — no throw on
 *       "already set". This is the sole protection against thread reuse with
 *       stale identity between cycles on the same scheduler thread.</li>
 *   <li>{@link #getOrThrow()} throws {@link IllegalStateException} with a
 *       diagnostic message when the slot is empty; a stack trace at the
 *       persister's {@code completeCollectionSet} means the decorator is not
 *       wired or the persister was invoked off-cycle.</li>
 *   <li>{@link #clear()} is idempotent ({@code ThreadLocal.remove()} on an
 *       empty slot is a no-op).</li>
 * </ul>
 *
 * <p>Scoped to a single {@code CollectableService.doCollection()} cycle,
 * which horizon runs synchronously on one scheduler thread. The decorator
 * sets on {@code execute()}; the persister reads in {@code completeCollectionSet}
 * and clears in the outer {@code finally} of that method.</p>
 */
public class AgentIdentityHolder {

    private final ThreadLocal<AgentIdentity> current = new ThreadLocal<>();

    public void set(int nodeId, String location) {
        current.set(new AgentIdentity(nodeId, location));
    }

    public AgentIdentity getOrThrow() {
        AgentIdentity id = current.get();
        if (id == null) {
            throw new IllegalStateException(
                    "AgentIdentity not populated — AgentIdentityCapturingCollectorClient not wired?");
        }
        return id;
    }

    public void clear() {
        current.remove();
    }
}
