/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.springframework.stereotype.Component;

/** Validates the raw rule list from {@code MaterializerProperties} against the JSON Schema. */
@Component
public class RetentionRulesValidator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public List<RetentionRule> validate(List<Map<String, Object>> raw) {
        JsonSchema schema = loadSchema();
        JsonNode root = MAPPER.valueToTree(raw);
        Set<ValidationMessage> errors = schema.validate(root);
        if (!errors.isEmpty()) {
            String msg = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining("; "));
            throw new IllegalStateException("Invalid retention rules: " + msg);
        }

        List<RetentionRule> rules = new ArrayList<>();
        for (Map<String, Object> r : raw) {
            try {
                rules.add(RetentionRule.fromMap(r));
            } catch (Exception e) {
                throw new IllegalStateException("Invalid retention rule '" + r.get("id") + "': " + e.getMessage(), e);
            }
        }
        return rules;
    }

    private JsonSchema loadSchema() {
        try (InputStream in = getClass().getResourceAsStream("/retention-schema.json")) {
            if (in == null) {
                throw new IllegalStateException("retention-schema.json not on classpath");
            }
            return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load retention-schema.json", e);
        }
    }
}
