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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Computes RFC 6902 JSON Patches between Twin state versions. Mirrors the
 * algorithm in horizon's {@link org.opennms.core.ipc.twin.common.AbstractTwinPublisher#getPatchValue}:
 * Jackson reads both states as JsonNode, JsonDiff.asJson computes the patch,
 * the result is serialized to bytes.
 *
 * <p>If either input fails to parse as JSON (corrupt state, schema mismatch),
 * returns null to signal "fall back to snapshot." The dispatcher (Task 8)
 * checks the return and emits a full snapshot in that case.
 */
@Component
public class TwinPatchGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TwinPatchGenerator.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * @return RFC 6902 JSON Patch as bytes, or null if either input is unparseable.
     */
    public ByteString diff(ByteString from, ByteString to) {
        try {
            JsonNode source = objectMapper.readTree(from.toByteArray());
            JsonNode target = objectMapper.readTree(to.toByteArray());
            JsonNode patch = JsonDiff.asJson(source, target);
            return ByteString.copyFromUtf8(patch.toString());
        } catch (Exception e) {
            LOG.warn("JSON Patch generation failed (from {} bytes -> to {} bytes); caller should fall back to snapshot",
                from.size(), to.size(), e);
            return null;
        }
    }

    /**
     * Applies an RFC 6902 JSON Patch to a base state, reconstructing the full
     * target state. Used to recover full Twin state from horizon's incremental
     * patch publishes ({@code is_patch_object=true}), which carry only the delta.
     *
     * @return the full state as bytes, or null if the base or patch is
     *         unparseable / the patch cannot be applied (caller drops the update).
     */
    public ByteString apply(ByteString base, ByteString patch) {
        try {
            JsonNode baseNode = objectMapper.readTree(base.toByteArray());
            JsonNode patchNode = objectMapper.readTree(patch.toByteArray());
            JsonNode result = JsonPatch.fromJson(patchNode).apply(baseNode);
            return ByteString.copyFromUtf8(result.toString());
        } catch (Exception e) {
            LOG.warn("JSON Patch application failed (base {} bytes, patch {} bytes); dropping update",
                base.size(), patch.size(), e);
            return null;
        }
    }
}
