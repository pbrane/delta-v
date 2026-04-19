# M2 — Collectd ServiceParameters identity-populate gap

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate Delta-V Collectd's Kafka publisher with real `nodeId` + `location` on every collection cycle, and fail loudly at publish time if identity is missing, so Phase 2 Prometheus Remote Write E2E passes end-to-end for the first time.

**Architecture:** Insert a Spring-managed `@Primary` decorator around horizon's `LocationAwareCollectorClient`. At every RPC dispatch the decorator captures `(nodeId, location)` into a `ThreadLocal`-backed `AgentIdentityHolder`. The existing `TimeseriesKafkaPersister` reads the holder at `completeCollectionSet()` time (same scheduler thread, synchronous), validates `nodeId > 0`, publishes, and clears the holder in a `try/finally`. No horizon modifications.

**Tech Stack:** Java 21, Spring Boot 4.0.3 (Spring 7.0), JUnit 5 Jupiter, Mockito, AssertJ, Maven 3.9.14 (via `./mvnw`), horizon 1.0.10 jars, Spring Cloud Stream Kafka binder (existing), SLF4J.

**Spec reference:** `docs/superpowers/specs/2026-04-19-m2-collectd-serviceparameters-identity-design.md`

---

## File map

All paths below are relative to the repo root `/Users/david/development/src/opennms/delta-v/`.

### Create

| Path | Responsibility |
|---|---|
| `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentity.java` | Immutable `record(int nodeId, String location)`. Compact constructor normalizes null location to `""`. No nodeId validation. |
| `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityHolder.java` | `ThreadLocal<AgentIdentity>` wrapper with `set`, `getOrThrow`, `clear`. |
| `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClient.java` | `@Primary` decorator implementing `LocationAwareCollectorClient`. Also contains the package-private nested `AgentIdentityCapturingCollectorRequestBuilder`. |
| `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityTest.java` | Record behavior: null/empty location handling, equality. |
| `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityHolderTest.java` | Set / get / clear / thread-isolation / overwrite semantics. |
| `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorRequestBuilderTest.java` | execute() sets holder, withXxx delegates, builder contract. |
| `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClientTest.java` | collect() returns decorated builder; other methods (none in this interface) — effectively a smoke test. |
| `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/AgentIdentityWiringIT.java` | Spring-context IT asserting decorator bean resolution and end-to-end holder capture. |

### Modify

| Path | Change |
|---|---|
| `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersister.java` | New constructor `(TimeseriesKafkaPublisher, String, AgentIdentityHolder)`. `parseIntOrZero`/`asString` deleted. `completeCollectionSet` rewritten with try/finally. |
| `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java` | Add `agentIdentityHolder` `@Bean`. Add `@Primary` decorator `@Bean`. Update `compositePersisterFactory` signature and `FanoutPersisterFactory` constructor to thread the holder. |
| `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersisterTest.java` | Rewrite: holder-based tests replace ServiceParameters-based tests. |
| `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/TimeseriesPublisherFeatureFlagOffIT.java` | One new test: decorator bean absent when flag off. |

---

## Phase 1 — Identity primitives

No dependencies on existing files. Each task creates source + test together (TDD). Safe to commit after each task.

### Task 1.1 — `AgentIdentity` record

**Files:**
- Create: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentity.java`
- Test: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityTest.java`

- [ ] **Step 1: Write the failing test**

```java
// core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityTest.java
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
```

- [ ] **Step 2: Run test to verify it fails (record class does not exist)**

Run: `./mvnw -pl core/daemon-boot-collectd -Dtest=AgentIdentityTest test`
Expected: compilation failure — `AgentIdentity` cannot be resolved.

- [ ] **Step 3: Write the record**

```java
// core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentity.java
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
 * Immutable agent identity captured from a {@code CollectionAgent} at RPC
 * dispatch time and consumed by {@code TimeseriesKafkaPersister} at publish
 * time.
 *
 * <p>Validation of {@code nodeId} is intentionally deferred to the persist
 * site (persist-time-only fail-fast): the record accepts any int so capture
 * never aborts a collection cycle. A {@code nodeId <= 0} surfaces as a
 * publisher-side {@code IllegalStateException} caught by
 * {@code FanoutPersister}, which increments the kafka failures counter.</p>
 *
 * <p>{@code location} is normalized to the empty string when null — Default
 * location setups sometimes return {@code null}. Downstream code may assume
 * non-null.</p>
 */
public record AgentIdentity(int nodeId, String location) {
    public AgentIdentity {
        if (location == null) {
            location = "";
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl core/daemon-boot-collectd -Dtest=AgentIdentityTest test`
Expected: `Tests run: 6, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 5: Commit**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentity.java \
        core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityTest.java
git commit -m "feat(collectd): add AgentIdentity record for per-cycle identity capture"
```

---

### Task 1.2 — `AgentIdentityHolder` ThreadLocal

**Files:**
- Create: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityHolder.java`
- Test: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityHolderTest.java`

- [ ] **Step 1: Write the failing test**

```java
// core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityHolderTest.java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl core/daemon-boot-collectd -Dtest=AgentIdentityHolderTest test`
Expected: compilation failure — `AgentIdentityHolder` cannot be resolved.

- [ ] **Step 3: Write the holder**

```java
// core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityHolder.java
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
                    "AgentIdentity not populated — LocationAwareCollectorClient decorator not wired?");
        }
        return id;
    }

    public void clear() {
        current.remove();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl core/daemon-boot-collectd -Dtest=AgentIdentityHolderTest test`
Expected: `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 5: Commit**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityHolder.java \
        core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityHolderTest.java
git commit -m "feat(collectd): add AgentIdentityHolder ThreadLocal for per-cycle identity"
```

---

## Phase 2 — Decorator

### Task 2.1 — `AgentIdentityCapturingCollectorClient` (and nested builder)

Builder + client live in the same file because the builder is the inner collaborator of the client and must only be constructable from within the client (enforced by `static` nested visibility). Two test classes exercise them separately.

**Files:**
- Create: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClient.java`
- Test: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorRequestBuilderTest.java`
- Test: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClientTest.java`

- [ ] **Step 1: Write the failing builder test**

```java
// core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorRequestBuilderTest.java
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectorRequestBuilder;
import org.opennms.netmgt.collection.api.ServiceCollector;

class AgentIdentityCapturingCollectorRequestBuilderTest {

    @Test
    void withAgentThenExecuteSetsHolderAndDelegates() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        CompletableFuture<CollectionSet> future = new CompletableFuture<>();
        when(delegate.execute()).thenReturn(future);
        when(delegate.withAgent(any())).thenReturn(delegate);

        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(42);
        when(agent.getLocationName()).thenReturn("Site-A");

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        CompletableFuture<CollectionSet> result = builder.withAgent(agent).execute();

        assertThat(result).isSameAs(future);
        assertThat(holder.getOrThrow().nodeId()).isEqualTo(42);
        assertThat(holder.getOrThrow().location()).isEqualTo("Site-A");
        verify(delegate).withAgent(agent);
        verify(delegate).execute();
    }

    @Test
    void executeWithoutAgentDoesNotSetHolderButStillDelegates() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.execute();

        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
        verify(delegate).execute();
    }

    @Test
    void agentWithZeroNodeIdCapturedAsIs() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.withAgent(any())).thenReturn(delegate);
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(0);
        when(agent.getLocationName()).thenReturn("Default");

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.withAgent(agent).execute();

        assertThat(holder.getOrThrow().nodeId()).isZero();
        assertThat(holder.getOrThrow().location()).isEqualTo("Default");
    }

    @Test
    void agentWithNullLocationCapturedAsEmptyString() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.withAgent(any())).thenReturn(delegate);
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(5);
        when(agent.getLocationName()).thenReturn(null);

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.withAgent(agent).execute();

        assertThat(holder.getOrThrow().nodeId()).isEqualTo(5);
        assertThat(holder.getOrThrow().location()).isEqualTo("");
    }

    @Test
    void withXxxMethodsReturnThisAndDelegate() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.withSystemId("sys")).thenReturn(delegate);
        when(delegate.withCollector(any())).thenReturn(delegate);
        when(delegate.withCollectorClassName("class")).thenReturn(delegate);
        when(delegate.withTimeToLive(1000L)).thenReturn(delegate);
        when(delegate.withAttribute("k", "v")).thenReturn(delegate);
        when(delegate.withAttributes(Map.of("a", "b"))).thenReturn(delegate);

        ServiceCollector svc = mock(ServiceCollector.class);
        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);

        assertThat(builder.withSystemId("sys")).isSameAs(builder);
        assertThat(builder.withCollector(svc)).isSameAs(builder);
        assertThat(builder.withCollectorClassName("class")).isSameAs(builder);
        assertThat(builder.withTimeToLive(1000L)).isSameAs(builder);
        assertThat(builder.withAttribute("k", "v")).isSameAs(builder);
        assertThat(builder.withAttributes(Map.of("a", "b"))).isSameAs(builder);

        verify(delegate).withSystemId("sys");
        verify(delegate).withCollector(svc);
        verify(delegate).withCollectorClassName("class");
        verify(delegate).withTimeToLive(1000L);
        verify(delegate).withAttribute("k", "v");
        verify(delegate).withAttributes(Map.of("a", "b"));
    }

    @Test
    void executeDoesNotInteractWithHolderIfAgentNeverSet() {
        // Verifies the no-agent path uses no ThreadLocal state at all.
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = mock(AgentIdentityHolder.class);
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.execute();

        verifyNoInteractions(holder);
    }
}
```

- [ ] **Step 2: Write the failing client test**

```java
// core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClientTest.java
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectorRequestBuilder;
import org.opennms.netmgt.collection.api.LocationAwareCollectorClient;

class AgentIdentityCapturingCollectorClientTest {

    @Test
    void collectDelegatesAndReturnsCapturingBuilder() {
        LocationAwareCollectorClient inner = mock(LocationAwareCollectorClient.class);
        CollectorRequestBuilder innerBuilder = mock(CollectorRequestBuilder.class);
        when(inner.collect()).thenReturn(innerBuilder);
        AgentIdentityHolder holder = new AgentIdentityHolder();

        AgentIdentityCapturingCollectorClient client =
                new AgentIdentityCapturingCollectorClient(inner, holder);

        CollectorRequestBuilder result = client.collect();

        assertThat(result).isInstanceOf(AgentIdentityCapturingCollectorClient.CapturingBuilder.class);
        verify(inner).collect();
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `./mvnw -pl core/daemon-boot-collectd -Dtest="AgentIdentityCapturing*Test" test`
Expected: compilation failure — `AgentIdentityCapturingCollectorClient` cannot be resolved.

- [ ] **Step 4: Write the client + nested builder**

```java
// core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClient.java
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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectorRequestBuilder;
import org.opennms.netmgt.collection.api.LocationAwareCollectorClient;
import org.opennms.netmgt.collection.api.ServiceCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spring {@code @Primary} decorator over horizon's
 * {@link LocationAwareCollectorClient}. Returns a {@link CapturingBuilder} from
 * {@link #collect()}; that builder captures the {@link CollectionAgent}'s
 * {@code nodeId} and {@code locationName} into an {@link AgentIdentityHolder}
 * just before the real RPC dispatches, so {@code TimeseriesKafkaPersister}
 * (constructed later in the same synchronous {@code doCollection()} cycle)
 * can read them.
 */
public class AgentIdentityCapturingCollectorClient implements LocationAwareCollectorClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(AgentIdentityCapturingCollectorClient.class);

    private final LocationAwareCollectorClient delegate;
    private final AgentIdentityHolder holder;

    public AgentIdentityCapturingCollectorClient(LocationAwareCollectorClient delegate,
                                                 AgentIdentityHolder holder) {
        this.delegate = delegate;
        this.holder = holder;
        LOG.info("Wrapping LocationAwareCollectorClient for identity capture; delegate = {}",
                delegate.getClass().getName());
    }

    @Override
    public CollectorRequestBuilder collect() {
        return new CapturingBuilder(delegate.collect(), holder);
    }

    /**
     * Delegating {@link CollectorRequestBuilder} that records
     * {@link AgentIdentity} into the holder at {@link #execute()} time.
     *
     * <p>Package-private so tests can construct it directly; the only
     * production construction site is {@link #collect()} on the outer class.</p>
     */
    static final class CapturingBuilder implements CollectorRequestBuilder {

        private final CollectorRequestBuilder delegate;
        private final AgentIdentityHolder holder;
        private CollectionAgent agent;

        CapturingBuilder(CollectorRequestBuilder delegate, AgentIdentityHolder holder) {
            this.delegate = delegate;
            this.holder = holder;
        }

        @Override
        public CollectorRequestBuilder withAgent(CollectionAgent agent) {
            this.agent = agent;
            delegate.withAgent(agent);
            return this;
        }

        @Override
        public CollectorRequestBuilder withSystemId(String systemId) {
            delegate.withSystemId(systemId);
            return this;
        }

        @Override
        public CollectorRequestBuilder withCollector(ServiceCollector collector) {
            delegate.withCollector(collector);
            return this;
        }

        @Override
        public CollectorRequestBuilder withCollectorClassName(String className) {
            delegate.withCollectorClassName(className);
            return this;
        }

        @Override
        public CollectorRequestBuilder withTimeToLive(Long ttlInMs) {
            delegate.withTimeToLive(ttlInMs);
            return this;
        }

        @Override
        public CollectorRequestBuilder withAttribute(String key, Object value) {
            delegate.withAttribute(key, value);
            return this;
        }

        @Override
        public CollectorRequestBuilder withAttributes(Map<String, Object> attributes) {
            delegate.withAttributes(attributes);
            return this;
        }

        @Override
        public CompletableFuture<CollectionSet> execute() {
            if (agent != null) {
                holder.set(agent.getNodeId(), agent.getLocationName());
            }
            return delegate.execute();
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./mvnw -pl core/daemon-boot-collectd -Dtest="AgentIdentityCapturing*Test" test`
Expected: `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0` across the two test classes.

- [ ] **Step 6: Commit**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClient.java \
        core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorRequestBuilderTest.java \
        core/daemon-boot-collectd/src/test/java/org/deltav/collectd/identity/AgentIdentityCapturingCollectorClientTest.java
git commit -m "feat(collectd): add AgentIdentityCapturingCollectorClient decorator"
```

---

## Phase 3 — Persister + Factory refactor (atomic)

This phase breaks the tree temporarily: changing `TimeseriesKafkaPersister`'s constructor breaks `FanoutPersisterFactory`'s `createPersister` calls and the `compositePersisterFactory` `@Bean` method. The commit must land all four source changes plus the test rewrite together.

### Task 3.1 — Add `agentIdentityHolder` `@Bean` (standalone, no breakage)

Landing this first means Phase 3.2's refactor can reference the bean without conflict.

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Add the bean method**

In `TimeseriesKafkaPublisherConfiguration.java`, after the existing `import` block add:

```java
import org.deltav.collectd.identity.AgentIdentityHolder;
```

After the `collectionSetToProtobufTranslator()` `@Bean` method, insert:

```java
    /**
     * Per-scheduler-thread holder for the {@code nodeId} + {@code location}
     * of the {@code CollectionAgent} currently being collected. Populated by
     * {@link org.deltav.collectd.identity.AgentIdentityCapturingCollectorClient}
     * at RPC dispatch and read by {@link TimeseriesKafkaPersister} at publish time.
     */
    @Bean
    public AgentIdentityHolder agentIdentityHolder() {
        return new AgentIdentityHolder();
    }
```

- [ ] **Step 2: Run module verify (nothing else changed yet — should still be green)**

Run: `./mvnw -pl core/daemon-boot-collectd -DskipITs verify`
Expected: `BUILD SUCCESS`. Unit tests pass (including the two Phase 1 tests already committed).

- [ ] **Step 3: Commit**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java
git commit -m "feat(collectd): register AgentIdentityHolder Spring bean"
```

---

### Task 3.2 — Refactor persister + factory + test (single commit)

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersister.java`
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`
- Modify: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersisterTest.java`

- [ ] **Step 1: Rewrite `TimeseriesKafkaPersisterTest.java`** (failing — constructor signature doesn't exist yet)

Replace the entire file with:

```java
// core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersisterTest.java
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
package org.deltav.collectd.timeseries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.deltav.collectd.identity.AgentIdentityHolder;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.AttributeGroup;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;

class TimeseriesKafkaPersisterTest {

    @Test
    void populatedHolderCausesPublishWithCapturedIdentityAndClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(42, "Site-A");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "critical-infra", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);
        persister.completeCollectionSet(set);

        verify(publisher).publish(eq(set), eq("critical-infra"), eq(42), eq("Site-A"));
        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void emptyHolderThrowsIllegalStateExceptionButStillClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);

        assertThatThrownBy(() -> persister.completeCollectionSet(set))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("AgentIdentity not populated");
        verifyNoInteractions(publisher);
        // idempotent clear — must not throw on already-empty slot:
        holder.clear();
    }

    @Test
    void holderWithZeroNodeIdThrowsAndClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(0, "Default");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);

        assertThatThrownBy(() -> persister.completeCollectionSet(set))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nodeId must be > 0, got 0");
        verifyNoInteractions(publisher);
        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void holderWithNegativeNodeIdThrowsWithActualValueInMessage() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(-5, "Default");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);

        assertThatThrownBy(() -> persister.completeCollectionSet(set))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("got -5");
    }

    @Test
    void completeCollectionSetWithoutVisitDoesNothingButStillClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(7, "X");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.completeCollectionSet(set);

        verifyNoInteractions(publisher);
        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void visitResourceVisitGroupVisitAttributeAreNoOps() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        persister.visitResource(mock(CollectionResource.class));
        persister.visitGroup(mock(AttributeGroup.class));
        persister.visitAttribute(mock(CollectionAttribute.class));
        persister.completeAttribute(mock(CollectionAttribute.class));
        persister.completeGroup(mock(AttributeGroup.class));
        persister.completeResource(mock(CollectionResource.class));

        verifyNoInteractions(publisher);
    }

    @Test
    void persistNumericAttributeAndPersistStringAttributeAreNoOps() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        persister.persistNumericAttribute(mock(CollectionAttribute.class));
        persister.persistStringAttribute(mock(CollectionAttribute.class));

        verifyNoInteractions(publisher);
    }
}
```

- [ ] **Step 2: Rewrite `TimeseriesKafkaPersister.java`**

Replace the entire file with:

```java
// core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersister.java
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
package org.deltav.collectd.timeseries;

import org.deltav.collectd.identity.AgentIdentity;
import org.deltav.collectd.identity.AgentIdentityHolder;
import org.opennms.netmgt.collection.api.AttributeGroup;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;

/**
 * Thin horizon {@link Persister} adapter that hands every {@link CollectionSet}
 * to the shared {@link TimeseriesKafkaPublisher} once, at
 * {@link #completeCollectionSet(CollectionSet)}. All per-attribute callbacks
 * are no-ops — the translator does its own walk.
 *
 * <p>Identity ({@code nodeId} + {@code location}) is captured by
 * {@code AgentIdentityCapturingCollectorClient} before each RPC dispatch and
 * read from the injected {@link AgentIdentityHolder} at publish time. If the
 * holder is empty (decorator not wired) or {@code nodeId <= 0}, this persister
 * throws {@link IllegalStateException}; {@code FanoutPersister} catches it,
 * increments {@code deltav.collectd.persister.kafka.failures{step=completeCollectionSet}},
 * and logs WARN. The inner persister path is unaffected.</p>
 *
 * <p>{@code collectionPackage} is extracted once by
 * {@code FanoutPersisterFactory.createPersister} from the horizon
 * {@code ServiceParameters} and passed as an explicit constructor argument.</p>
 */
public class TimeseriesKafkaPersister implements Persister {

    private final TimeseriesKafkaPublisher publisher;
    private final String collectionPackage;
    private final AgentIdentityHolder holder;
    private CollectionSet capturedSet;

    public TimeseriesKafkaPersister(TimeseriesKafkaPublisher publisher,
                                     String collectionPackage,
                                     AgentIdentityHolder holder) {
        this.publisher = publisher;
        this.collectionPackage = collectionPackage;
        this.holder = holder;
    }

    @Override
    public void visitCollectionSet(CollectionSet set) {
        this.capturedSet = set;
    }

    @Override
    public void visitResource(CollectionResource resource) { /* no-op */ }

    @Override
    public void visitGroup(AttributeGroup group) { /* no-op */ }

    @Override
    public void visitAttribute(CollectionAttribute attribute) { /* no-op */ }

    @Override
    public void completeAttribute(CollectionAttribute attribute) { /* no-op */ }

    @Override
    public void completeGroup(AttributeGroup group) { /* no-op */ }

    @Override
    public void completeResource(CollectionResource resource) { /* no-op */ }

    @Override
    public void completeCollectionSet(CollectionSet set) {
        try {
            if (capturedSet != null) {
                AgentIdentity identity = holder.getOrThrow();
                if (identity.nodeId() <= 0) {
                    throw new IllegalStateException(
                            "Invalid agent identity for Kafka publish: nodeId must be > 0, got "
                                    + identity.nodeId());
                }
                publisher.publish(capturedSet, collectionPackage,
                        identity.nodeId(), identity.location());
            }
        } finally {
            holder.clear();
            capturedSet = null;
        }
    }

    @Override
    public void persistNumericAttribute(CollectionAttribute attribute) { /* no-op */ }

    @Override
    public void persistStringAttribute(CollectionAttribute attribute) { /* no-op */ }
}
```

- [ ] **Step 3: Update `TimeseriesKafkaPublisherConfiguration.java`'s `FanoutPersisterFactory` and `compositePersisterFactory` `@Bean`**

Find the `compositePersisterFactory` `@Bean` method and replace the whole method plus the nested `FanoutPersisterFactory` class with:

```java
    /**
     * Composite factory that fans out each createPersister() call to both the
     * existing InMemoryStorage-backed TimeseriesPersisterFactory and a fresh
     * TimeseriesKafkaPersister. Declared @Primary so Collectd's constructor
     * resolves to this bean when the feature flag is on.
     */
    @Bean
    @Primary
    public PersisterFactory compositePersisterFactory(
            @Qualifier("timeseriesPersisterFactory") PersisterFactory innerFactory,
            TimeseriesKafkaPublisher publisher,
            AgentIdentityHolder agentIdentityHolder,
            MeterRegistry meterRegistry,
            @Value("${deltav.collectd.persister.inner.fail-fast:false}") boolean failFastInner,
            @Value("${deltav.collectd.persister.kafka.fail-fast:false}") boolean failFastKafka) {
        LOG.info("Creating compositePersisterFactory wrapping inner={}@{} (failFastInner={}, failFastKafka={})",
                innerFactory.getClass().getName(),
                System.identityHashCode(innerFactory),
                failFastInner, failFastKafka);
        return new FanoutPersisterFactory(innerFactory, publisher, agentIdentityHolder, meterRegistry,
                failFastInner, failFastKafka);
    }

    /**
     * Package-private composite factory. Kept as a static nested class so the
     * public API of this module remains the four files listed in the spec.
     */
    static final class FanoutPersisterFactory implements PersisterFactory {
        private final PersisterFactory innerFactory;
        private final TimeseriesKafkaPublisher publisher;
        private final AgentIdentityHolder holder;
        private final MeterRegistry meterRegistry;
        private final boolean failFastInner;
        private final boolean failFastKafka;

        FanoutPersisterFactory(PersisterFactory innerFactory, TimeseriesKafkaPublisher publisher,
                               AgentIdentityHolder holder,
                               MeterRegistry meterRegistry,
                               boolean failFastInner, boolean failFastKafka) {
            this.innerFactory = innerFactory;
            this.publisher = publisher;
            this.holder = holder;
            this.meterRegistry = meterRegistry;
            this.failFastInner = failFastInner;
            this.failFastKafka = failFastKafka;
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository),
                    new TimeseriesKafkaPersister(publisher, extractCollection(params), holder),
                    meterRegistry, failFastInner, failFastKafka);
        }

        @Override
        public Persister createPersister(ServiceParameters params, RrdRepository repository,
                                         boolean dontPersistCounters, boolean forceStoreByGroup,
                                         boolean dontReorderAttributes) {
            return new FanoutPersister(
                    innerFactory.createPersister(params, repository, dontPersistCounters,
                            forceStoreByGroup, dontReorderAttributes),
                    new TimeseriesKafkaPersister(publisher, extractCollection(params), holder),
                    meterRegistry, failFastInner, failFastKafka);
        }

        private static String extractCollection(ServiceParameters params) {
            Object raw = params.getParameters().get("collection");
            return raw == null ? "default" : raw.toString();
        }
    }
```

- [ ] **Step 4: Run the persister unit tests to verify they pass**

Run: `./mvnw -pl core/daemon-boot-collectd -Dtest=TimeseriesKafkaPersisterTest test`
Expected: `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 5: Run the full module verify (all unit tests + existing ITs)**

Run: `./mvnw -pl core/daemon-boot-collectd -DskipITs verify`
Expected: `BUILD SUCCESS`. All unit tests pass. ITs are skipped.

- [ ] **Step 6: Commit**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersister.java \
        core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java \
        core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/TimeseriesKafkaPersisterTest.java
git commit -m "$(cat <<'EOF'
refactor(collectd): TimeseriesKafkaPersister reads identity from holder

Constructor becomes (publisher, collectionPackage, holder). The persister
validates nodeId > 0 at completeCollectionSet time; an empty holder or
invalid identity throws IllegalStateException caught by FanoutPersister.
ServiceParameters-based parseIntOrZero defaults removed — identity now
flows from AgentIdentityCapturingCollectorClient via ThreadLocal.

FanoutPersisterFactory threads the AgentIdentityHolder through to each
constructed persister and extracts the static "collection" string from
ServiceParameters once per createPersister call.
EOF
)"
```

---

## Phase 4 — Decorator wiring + feature-flag-off assertion

### Task 4.1 — Register the `@Primary` decorator bean

**Files:**
- Modify: `core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java`

- [ ] **Step 1: Add the decorator bean method**

In `TimeseriesKafkaPublisherConfiguration.java`, add these imports:

```java
import org.deltav.collectd.identity.AgentIdentityCapturingCollectorClient;
import org.opennms.netmgt.collection.api.LocationAwareCollectorClient;
```

After the `agentIdentityHolder()` `@Bean` method (added in Task 3.1), insert:

```java
    /**
     * {@code @Primary} decorator over horizon's {@link LocationAwareCollectorClient}
     * that captures {@code (nodeId, location)} into {@link AgentIdentityHolder}
     * before every RPC dispatch, so {@link TimeseriesKafkaPersister} can read
     * identity at publish time.
     *
     * <p>{@code CollectdRpcConfiguration} registers the original bean under the
     * default name {@code locationAwareCollectorClient}; Spring Boot 4 disables
     * bean-definition overriding, so the decorator uses a distinct method name
     * and is made primary. The {@code @Qualifier} on the delegate parameter
     * selects the horizon bean (not this decorator, which would cause infinite
     * recursion).</p>
     */
    @Bean
    @Primary
    public LocationAwareCollectorClient agentIdentityCapturingCollectorClient(
            @Qualifier("locationAwareCollectorClient") LocationAwareCollectorClient inner,
            AgentIdentityHolder holder) {
        return new AgentIdentityCapturingCollectorClient(inner, holder);
    }
```

- [ ] **Step 2: Run module verify**

Run: `./mvnw -pl core/daemon-boot-collectd -DskipITs verify`
Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit**

```bash
git add core/daemon-boot-collectd/src/main/java/org/deltav/collectd/timeseries/TimeseriesKafkaPublisherConfiguration.java
git commit -m "feat(collectd): register @Primary LocationAwareCollectorClient decorator"
```

---

### Task 4.2 — Extend `TimeseriesPublisherFeatureFlagOffIT`

**Files:**
- Modify: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/TimeseriesPublisherFeatureFlagOffIT.java`

- [ ] **Step 1: Add test + import**

Add this import near the top:

```java
import org.deltav.collectd.identity.AgentIdentityCapturingCollectorClient;
import org.deltav.collectd.identity.AgentIdentityHolder;
```

At the bottom of the class, before the closing brace, add:

```java
    @Test
    void agentIdentityCapturingClientBeanIsAbsentWhenFlagOff() {
        // Feature-flag off ⇒ decorator not registered ⇒ horizon's raw
        // LocationAwareCollectorClient wins. This test doesn't try to resolve
        // LocationAwareCollectorClient directly (the CollectdRpcConfiguration
        // bean isn't on this minimal test's classpath) — it just asserts the
        // decorator type is not instantiated.
        assertThat(ctx.getBeanNamesForType(AgentIdentityCapturingCollectorClient.class)).isEmpty();
    }

    @Test
    void agentIdentityHolderBeanIsAbsentWhenFlagOff() {
        assertThat(ctx.getBeanNamesForType(AgentIdentityHolder.class)).isEmpty();
    }
```

- [ ] **Step 2: Run the IT**

Run: `./mvnw -pl core/daemon-boot-collectd -Dit.test=TimeseriesPublisherFeatureFlagOffIT verify`
Expected: all tests pass (3 existing + 2 new = 5).

- [ ] **Step 3: Commit**

```bash
git add core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/TimeseriesPublisherFeatureFlagOffIT.java
git commit -m "test(collectd): assert identity capture beans absent when feature flag off"
```

---

## Phase 5 — Spring-wiring integration test

### Task 5.1 — `AgentIdentityWiringIT`

This IT follows the `CollectdApplicationScanIT` pattern but with a narrower scope: it asserts bean resolution and the captureflow. To keep it independent of `CollectdApplication`'s full scan, it uses an `@Import` style similar to `TimeseriesPublisherFeatureFlagOffIT` but with the flag on, and stubs a minimal `LocationAwareCollectorClient` delegate.

**Files:**
- Create: `core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/AgentIdentityWiringIT.java`

- [ ] **Step 1: Write the IT**

```java
// core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/AgentIdentityWiringIT.java
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
package org.deltav.collectd.timeseries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.deltav.collectd.identity.AgentIdentity;
import org.deltav.collectd.identity.AgentIdentityCapturingCollectorClient;
import org.deltav.collectd.identity.AgentIdentityHolder;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectorRequestBuilder;
import org.opennms.netmgt.collection.api.LocationAwareCollectorClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

/**
 * Spring-wiring integration test asserting that the {@code @Primary}
 * decorator wins bean resolution for {@link LocationAwareCollectorClient}
 * and that its {@code execute()} actually populates the
 * {@link AgentIdentityHolder}.
 *
 * <p>Stubs the horizon delegate with a simple test bean named
 * {@code locationAwareCollectorClient} (the name the decorator's
 * {@code @Qualifier} expects). Does not start the real
 * {@link org.deltav.netmgt.collectd.boot.CollectdApplication} — that live
 * scan is covered by {@code CollectdApplicationScanIT}.</p>
 */
@SpringBootTest(classes = {
        AgentIdentityWiringIT.TestApp.class
})
@TestPropertySource(properties = {
        "deltav.timeseries.enabled=true",
        "spring.main.web-application-type=none",
        "spring.main.banner-mode=off"
})
class AgentIdentityWiringIT {

    @EnableAutoConfiguration
    @Import({
            TimeseriesKafkaPublisherConfiguration.class,
            TestChannelBinderConfiguration.class
    })
    static class TestApp {

        /**
         * Stub horizon delegate registered under the default name
         * {@code locationAwareCollectorClient}. The decorator @Qualifier
         * pulls this bean in as its delegate.
         */
        @Bean(name = "locationAwareCollectorClient")
        LocationAwareCollectorClient innerClient() {
            LocationAwareCollectorClient inner = mock(LocationAwareCollectorClient.class);
            CollectorRequestBuilder builder = mock(CollectorRequestBuilder.class);
            when(inner.collect()).thenReturn(builder);
            when(builder.withAgent(org.mockito.ArgumentMatchers.any())).thenReturn(builder);
            when(builder.execute()).thenReturn(new CompletableFuture<>());
            return inner;
        }

        /**
         * {@code TimeseriesKafkaPublisherConfiguration.compositePersisterFactory}
         * injects a {@code @Qualifier("timeseriesPersisterFactory")}
         * {@link org.opennms.netmgt.collection.api.PersisterFactory}. In this
         * narrow IT we don't boot the full JPA/TSS chain that would normally
         * create it, so we provide a minimal stub.
         */
        @Bean(name = "timeseriesPersisterFactory")
        org.opennms.netmgt.collection.api.PersisterFactory innerPersisterFactory() {
            return mock(org.opennms.netmgt.collection.api.PersisterFactory.class);
        }
    }

    @Autowired
    ApplicationContext ctx;

    @Autowired
    LocationAwareCollectorClient injectedClient;

    @Autowired
    AgentIdentityHolder holder;

    @Test
    void primaryBeanIsTheCapturingDecorator() {
        assertThat(injectedClient).isInstanceOf(AgentIdentityCapturingCollectorClient.class);
    }

    @Test
    void holderBeanIsPresent() {
        assertThat(ctx.getBean(AgentIdentityHolder.class)).isSameAs(holder);
    }

    @Test
    void decoratorExecuteCapturesAgentIdentity() {
        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(123);
        when(agent.getLocationName()).thenReturn("lab-A");

        CompletableFuture<CollectionSet> future =
                injectedClient.collect().withAgent(agent).execute();

        assertThat(future).isNotNull();
        AgentIdentity captured = holder.getOrThrow();
        assertThat(captured.nodeId()).isEqualTo(123);
        assertThat(captured.location()).isEqualTo("lab-A");
        holder.clear();   // cleanup — test runs on a shared executor thread
    }
}
```

- [ ] **Step 2: Run the IT**

Run: `./mvnw -pl core/daemon-boot-collectd -Dit.test=AgentIdentityWiringIT verify`
Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add core/daemon-boot-collectd/src/test/java/org/deltav/collectd/timeseries/AgentIdentityWiringIT.java
git commit -m "test(collectd): AgentIdentityWiringIT asserts @Primary decorator + holder capture"
```

---

## Phase 6 — Verification and PR

### Task 6.1 — Module-level green build

- [ ] **Step 1: Full module verify (unit + IT)**

Run: `./mvnw -pl core/daemon-boot-collectd verify`
Expected: `BUILD SUCCESS`. All unit tests + all ITs pass (including the two new ITs and the existing `CollectdApplicationScanIT` which the decorator should NOT break — that IT mocks `LocationAwareCollectorClient` directly via `@MockitoBean`, which overrides even `@Primary`, so the decorator should simply not be in its bean graph).

If `CollectdApplicationScanIT` fails due to our decorator trying to wrap the mock, investigate immediately — this is a Q2 regression guard. The fix is likely to exclude our decorator bean under the test's property set, or confirm `@MockitoBean` precedence holds. **Do not push a workaround that weakens the production wiring.**

---

### Task 6.2 — Full-reactor verify

Per `feedback_delta_v_full_reactor_verify`: module-only builds can miss sibling-module compilation failures.

- [ ] **Step 1: Full-reactor clean install**

Run: `./mvnw -B -DskipTests -fae clean install`
Expected: `BUILD SUCCESS`. No unrelated module breakage from the `core/daemon-boot-collectd` changes.

If any sibling module references `TimeseriesKafkaPersister` directly, this catches it (unlikely — that class is daemon-boot-only).

---

### Task 6.3 — Local Phase 2 E2E verification

This is the acceptance gate. Not in CI; run locally before opening the PR.

- [ ] **Step 1: Ensure no leftover delta-v stack**

Run: `docker ps --format '{{.Names}}' | grep -iE "delta|opennms" || echo "clean"`
Expected: `clean`, or teardown any leftovers with `./opennms-container/delta-v/build.sh down`.

- [ ] **Step 2: Rebuild daemon-boot JARs** (per `feedback_rebuild_all_daemons`: rebuild ALL 12 daemon boots before `build.sh deltav`)

Run: `bash -c './build.sh daemons' &` then `disown`  (per `Env quirk from PR #175` — strict-mode Bash wrapping kills `build.sh` with 255).

Wait for completion (tail the log file produced by the script). Expected: all 12 daemon boot JARs rebuilt, no errors.

- [ ] **Step 3: Bring up the delta-v stack**

Run: `bash -c './opennms-container/delta-v/build.sh deltav' &` then `disown`.
Wait until all containers are healthy (`docker ps` shows `(healthy)` for each).

- [ ] **Step 4: Run Phase 2 E2E**

Run: `./opennms-container/delta-v/test-prometheus-writer-e2e.sh`
Expected: exit 0, last line `ALL ASSERTIONS PASSED`.

- [ ] **Step 5: Verify `enrichment_lookup_fallback_total` stayed at 0**

Run:
```bash
curl -s http://localhost:<prometheus-writer-port>/actuator/prometheus \
  | grep enrichment_lookup_fallback_total
```
Expected: counter value is 0 (no fallback triggered = Collectd now emits correct location).

Capture the full E2E log tail and the counter curl output — attach to the PR description.

- [ ] **Step 6: Tear down the stack**

Run: `./opennms-container/delta-v/build.sh down`
Expected: all containers removed.

---

### Task 6.4 — Open the PR

- [ ] **Step 1: Push the branch**

Run: `git push -u origin fix/collectd-serviceparameters-identity`
Expected: branch published on `pbrane/delta-v`.

- [ ] **Step 2: Open the PR** (per `feedback_never_pr_opennms`)

Run:
```bash
gh pr create --repo pbrane/delta-v --base develop \
    --title "fix(collectd): populate ServiceParameters identity via LocationAwareCollectorClient decorator" \
    --body "$(cat <<'EOF'
## Summary

- Producer-side fix for Phase 2 E2E blocker: Collectd was publishing every `TimeseriesBatch` record with `nodeId=0 + location=""`, causing every consumer enrichment to miss.
- Introduces a `@Primary` `LocationAwareCollectorClient` decorator that captures `(nodeId, location)` into a `ThreadLocal`-backed `AgentIdentityHolder` at RPC dispatch. `TimeseriesKafkaPersister` reads the holder at `completeCollectionSet()`, validates `nodeId > 0`, publishes, and clears the holder in a `try/finally`.
- Persist-time-only fail-fast — a missing or invalid identity throws `IllegalStateException` caught by `FanoutPersister`, incrementing `deltav.collectd.persister.kafka.failures{step=completeCollectionSet}` (alert on `rate > 0`). Inner RRD/Newts persister path is unaffected.

## Acceptance

- `./mvnw -pl core/daemon-boot-collectd verify` — green (unit + IT).
- `./mvnw -B -DskipTests -fae clean install` — full-reactor green.
- `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` — **exit 0 with `ALL ASSERTIONS PASSED`** (first successful full Phase 2 E2E).
- `enrichment_lookup_fallback_total` stayed at 0 throughout the E2E run.

## Test plan

- [ ] CI green on the PR.
- [ ] Reviewer re-runs `./opennms-container/delta-v/test-prometheus-writer-e2e.sh` locally if they want to independently verify the E2E.
- [ ] `AgentIdentityWiringIT` asserts `@Primary` resolution and end-to-end capture.
- [ ] `TimeseriesKafkaPersisterTest` covers populated-holder, empty-holder, `nodeId <= 0`, and no-visit paths.
- [ ] `TimeseriesPublisherFeatureFlagOffIT` asserts both new beans are absent when `deltav.timeseries.enabled=false`.

## Design

- Spec: `docs/superpowers/specs/2026-04-19-m2-collectd-serviceparameters-identity-design.md`
- Plan: `docs/superpowers/plans/2026-04-19-m2-collectd-serviceparameters-identity.md`

## Memory updates (post-merge)

- `project_collectd_serviceparameters_identity_gap` — flip status OPEN → RESOLVED (delta-v#<PR>).
- `project_phase2_prometheus_writer_done` — addendum: "E2E passes end-to-end as of delta-v#<PR>."
EOF
)"
```

Expected: PR URL printed. Captured E2E log + counter output pasted into the PR description as evidence.

- [ ] **Step 3: Verify the PR is on the right repo**

Run: `gh pr view --repo pbrane/delta-v <PR-number>`
Expected: `baseRepository: pbrane/delta-v`, base branch `develop`. If base repo shows `OpenNMS/opennms`, abort and re-create (per `feedback_never_pr_opennms`).

---

### Task 6.5 — Post-merge memory updates

Runs only after the PR merges. Update two memory files.

- [ ] **Step 1: Flip the primary memo from OPEN to RESOLVED**

Edit `/Users/david/.claude/projects/-Users-david-development-src-opennms-delta-v/memory/project_collectd_serviceparameters_identity_gap.md`:

- Change `**Status (2026-04-18):** OPEN.` to `**Status:** RESOLVED (delta-v#<PR>).`
- Add a final paragraph under "References":
  ```
  - Resolved: delta-v#<PR> shipped LocationAwareCollectorClient decorator + AgentIdentityHolder ThreadLocal, threading nodeId/location into TimeseriesKafkaPersister. Phase 2 E2E now passes end-to-end.
  ```

Also update `MEMORY.md` entry to: `— RESOLVED (delta-v#<PR>): LocationAwareCollectorClient decorator threads identity via ThreadLocal; Phase 2 E2E fully green`.

- [ ] **Step 2: Update Phase 2 memo with addendum**

Edit `project_phase2_prometheus_writer_done.md` and append:

```
## 2026-04-19 addendum

E2E now passes end-to-end as of delta-v#<PR> (M2 producer fix). `enrichment_lookup_fallback_total` stays at 0 throughout; Collectd emits real nodeId + location via the AgentIdentityHolder path.
```

- [ ] **Step 3: No commit needed** — memory files are user-private, auto-persisted.

---

## Self-review

**Spec coverage walkthrough:**

- §2 acceptance: all 8 bullets are covered by Phase 6 tasks.
- §3 architecture: new beans in Phase 1/2/3.1/4.1; persister refactor in Phase 3.2.
- §3.1 Spring wiring snippet: Task 4.1 uses the exact `@Primary` + `@Qualifier("locationAwareCollectorClient")` pattern.
- §3.2 holder contract: Task 1.2 test cases assert each invariant (set, getOrThrow, clear, idempotency, thread isolation, overwrite).
- §3.3 AgentIdentity contract: Task 1.1 test asserts null-normalization + no nodeId validation.
- §4 data flow: Task 5.1 IT drives it end-to-end.
- §4.1 invariants: same-thread (synchronous RPC) — IT asserts capture + immediate read. Single capture point — decorator is the only caller of `holder.set`. Holder lifetime — Task 3.2 test "empty holder throws" + "completed set clears" paths.
- §4.2 edge cases: Task 3.2 tests cover empty holder (decorator-not-wired), `nodeId == 0` (malformed), `completeCollectionSet` without prior visit (null capturedSet), `nodeId == -1` (negative).
- §5 error handling: Task 3.2 steps 1-2 test the exact `IllegalStateException` + try/finally code from the spec.
- §6 observability: reuses pre-existing counter; no new metric code needed. INFO log line in `AgentIdentityCapturingCollectorClient` constructor is in Task 2.1.
- §7 component list: all 4 new files + 2 modified covered across Phases 1-4.
- §8 testing: all 5 new test classes + 1 rewrite + 1 extension in Phases 1-5.
- §9 rollout: Task 6.4 follows all guardrails (branch name, `--repo pbrane/delta-v`, mvnw, conventional commits).
- §10 post-merge: Task 6.5.
- §11 risks: Task 6.1's note about `CollectdApplicationScanIT` is the explicit `@MockitoBean` precedence check for the "Primary conflicts" risk. ThreadLocal leak risk is covered by Task 1.2 tests. Async-thread risk is documented by Task 5.1's "immediate read after execute" assertion.

**Placeholder scan:** Every code block is complete Java. Test code blocks assert concrete behavior, not "test above". No TBD/TODO. Commands have explicit expected output.

**Type consistency check:** `AgentIdentity.nodeId()` / `.location()` record accessors used consistently across all test files and the persister. `AgentIdentityHolder.set(int, String)`, `.getOrThrow()`, `.clear()` — same signatures everywhere. `AgentIdentityCapturingCollectorClient.CapturingBuilder` — used in both the client's `collect()` return and test direct construction. `TimeseriesKafkaPersister(TimeseriesKafkaPublisher, String, AgentIdentityHolder)` — matches Spring `@Bean` wiring and test construction. `FanoutPersisterFactory(innerFactory, publisher, holder, meterRegistry, failFastInner, failFastKafka)` — constructor signature matches in both the `compositePersisterFactory` `@Bean` and internal `createPersister` calls.

No gaps found.
