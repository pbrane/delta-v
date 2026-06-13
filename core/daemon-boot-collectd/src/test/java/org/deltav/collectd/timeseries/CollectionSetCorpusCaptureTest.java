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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionSet;

/**
 * Gated capture test: runs only when {@code -Dcorpus.out=<path>} is set.
 *
 * <p>Executes the live {@link CollectionSetToProtobufTranslator} against the
 * canonical interfaceSnmp fixture (the same values asserted in
 * {@link CollectionSetToProtobufTranslatorTest#interfaceScopedResourceEmitsInstanceAndResourceIdCorrectly}),
 * then writes {@code batch.getResources(0).toByteArray()} to the path given by
 * the system property so that the Rusty Minion contract repo can decode it as
 * its own {@code Resource} message (wire-identical field numbers).</p>
 *
 * <p>In normal CI the assumption fires immediately and the test is skipped —
 * no dependency on the unpublished contract artifact is introduced.</p>
 *
 * <p>To capture:</p>
 * <pre>{@code
 * ./mvnw -pl :org.opennms.core.daemon-boot-collectd test \
 *   -Dtest=CollectionSetCorpusCaptureTest \
 *   -Dcorpus.out=/path/to/opennms-ipc-contract/corpus/resource_from_java.bin
 * }</pre>
 */
class CollectionSetCorpusCaptureTest {

    @Test
    void captureTranslatorResourceBytesForRustyMinionContract() throws Exception {
        Assumptions.assumeTrue(
                System.getProperty("corpus.out") != null,
                "set -Dcorpus.out=<path> to capture; skipped in normal CI"
        );

        // Build the canonical interfaceSnmp fixture — values identical to those
        // asserted in interfaceScopedResourceEmitsInstanceAndResourceIdCorrectly.
        CollectionAttribute attr = CollectionSetToProtobufTranslatorTest.numericAttribute(
                "ifInOctets",
                org.opennms.netmgt.collection.api.AttributeType.COUNTER,
                1000L
        );
        CollectionSet set = CollectionSetToProtobufTranslatorTest.oneResourceSet(
                7, "Default", "interfaceSnmp", "eth0",
                "mib2-interfaces", List.of(attr), 1700000000000L
        );

        TimeseriesBatch batch = new CollectionSetToProtobufTranslator()
                .translate(set, "default", 7, "Default");

        // Fail loudly if the translator produced garbage before writing the file.
        assertThat(batch.getResourcesCount()).isEqualTo(1);
        Resource resource = batch.getResources(0);
        assertThat(resource.getType()).isEqualTo("interfaceSnmp");
        assertThat(resource.getInstance()).isEqualTo("eth0");
        assertThat(resource.getResourceId()).contains("interfaceSnmp[eth0]");

        // Serialize and write. The contract repo decodes these bytes as its own
        // Resource (field numbers are wire-identical).
        byte[] bytes = resource.toByteArray();
        Path out = Path.of(System.getProperty("corpus.out"));
        Files.createDirectories(out.getParent());
        Files.write(out, bytes);

        System.out.printf(
                "[CollectionSetCorpusCaptureTest] wrote %d bytes to %s%n",
                bytes.length, out.toAbsolutePath()
        );
    }
}
