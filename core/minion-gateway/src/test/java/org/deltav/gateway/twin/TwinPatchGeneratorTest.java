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
package org.deltav.gateway.twin;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TwinPatchGeneratorTest {

    @Test
    void identicalStates_returnsEmptyPatch() {
        TwinPatchGenerator gen = new TwinPatchGenerator();
        ByteString same = ByteString.copyFromUtf8("{\"x\":1}");

        ByteString patch = gen.diff(same, same);
        assertThat(patch.toStringUtf8()).isEqualTo("[]");
    }

    @Test
    void simpleFieldChange_producesReplaceOp() {
        TwinPatchGenerator gen = new TwinPatchGenerator();
        ByteString from = ByteString.copyFromUtf8("{\"x\":1}");
        ByteString to   = ByteString.copyFromUtf8("{\"x\":2}");

        String patch = gen.diff(from, to).toStringUtf8();
        assertThat(patch).contains("\"op\":\"replace\"").contains("\"path\":\"/x\"").contains("\"value\":2");
    }

    @Test
    void addField_producesAddOp() {
        TwinPatchGenerator gen = new TwinPatchGenerator();
        ByteString from = ByteString.copyFromUtf8("{\"x\":1}");
        ByteString to   = ByteString.copyFromUtf8("{\"x\":1,\"y\":2}");

        String patch = gen.diff(from, to).toStringUtf8();
        assertThat(patch).contains("\"op\":\"add\"").contains("\"path\":\"/y\"").contains("\"value\":2");
    }

    @Test
    void removeField_producesRemoveOp() {
        TwinPatchGenerator gen = new TwinPatchGenerator();
        ByteString from = ByteString.copyFromUtf8("{\"x\":1,\"y\":2}");
        ByteString to   = ByteString.copyFromUtf8("{\"x\":1}");

        String patch = gen.diff(from, to).toStringUtf8();
        assertThat(patch).contains("\"op\":\"remove\"").contains("\"path\":\"/y\"");
    }

    @Test
    void invalidJson_returnsNullToSignalSnapshotFallback() {
        TwinPatchGenerator gen = new TwinPatchGenerator();
        ByteString from = ByteString.copyFromUtf8("not valid json");
        ByteString to   = ByteString.copyFromUtf8("{\"x\":1}");

        ByteString patch = gen.diff(from, to);
        assertThat(patch).isNull();
    }

    @Test
    void apply_reconstructsFullStateFromHorizonPatch() {
        TwinPatchGenerator gen = new TwinPatchGenerator();
        ByteString base = ByteString.copyFromUtf8("{\"AWS\":\"Up\"}");
        ByteString target = ByteString.copyFromUtf8("{\"AWS\":\"Down\"}");

        // Horizon publishes the JSON-Patch delta; the gateway must apply it to the
        // cached base to recover the full state before broadcasting to Minions.
        ByteString patch = gen.diff(base, target);
        ByteString applied = gen.apply(base, patch);

        assertThat(applied.toStringUtf8()).contains("\"AWS\":\"Down\"");
    }

    @Test
    void apply_invalidPatch_returnsNull() {
        TwinPatchGenerator gen = new TwinPatchGenerator();
        ByteString base = ByteString.copyFromUtf8("{\"x\":1}");
        ByteString notAPatch = ByteString.copyFromUtf8("{\"not\":\"a patch array\"}");

        assertThat(gen.apply(base, notAPatch)).isNull();
    }
}
