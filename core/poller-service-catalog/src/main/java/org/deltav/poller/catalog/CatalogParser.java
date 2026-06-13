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
package org.deltav.poller.catalog;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.yaml.snakeyaml.LoaderOptions;

/**
 * Strict YAML reader for the flat poller service catalog (FR1 / D1).
 *
 * <p><strong>Strict by design:</strong> unknown properties are a hard failure. This is our
 * own schema, so leniency is pure downside — an {@code intervall:} typo would silently take
 * a default. A single {@code CatalogParser} instance is the shared parser configuration the
 * lint CLI and the daemon loader both use, so the two can never disagree on what parses.
 *
 * <p>This class only reads structure into the {@link Catalog} model. Structurally-malformed
 * input (bad YAML, unknown keys, wrong value types) throws here — that is the "malformed
 * catalog → context failure" path, deliberately distinct from {@link CatalogValidator}'s
 * collect-all semantic checks on a successfully parsed model.
 */
public final class CatalogParser {

    private final ObjectMapper mapper;

    public CatalogParser() {
        this.mapper = newStrictMapper();
    }

    /**
     * Builds the strict {@link ObjectMapper} shared by lint and loader. Unknown properties
     * fail (catches {@code intervall:}-style typos) and duplicate keys fail (a duplicated
     * {@code name:} silently taking last-wins is exactly the kind of leniency this schema
     * rejects). All other defaults are Jackson's.
     */
    static ObjectMapper newStrictMapper() {
        final LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setAllowDuplicateKeys(false);
        final YAMLFactory yamlFactory = YAMLFactory.builder()
                .loaderOptions(loaderOptions)
                .build();
        return new ObjectMapper(yamlFactory)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Parses a catalog file.
     *
     * @param file path to {@code poller-services.yaml}
     * @return the parsed catalog
     * @throws IOException on I/O failure or malformed/strict-violating YAML (the latter as a
     *                     {@link com.fasterxml.jackson.core.JsonProcessingException} whose
     *                     message names the offending key and location)
     */
    public Catalog parse(final Path file) throws IOException {
        try (Reader reader = Files.newBufferedReader(file)) {
            return parse(reader);
        }
    }

    /**
     * Parses a catalog from an open reader (used by tests and the lint CLI's stream paths).
     *
     * @param reader YAML source; the caller owns closing it
     * @return the parsed catalog
     * @throws IOException on read failure or malformed/strict-violating YAML
     */
    public Catalog parse(final Reader reader) throws IOException {
        final Catalog catalog = mapper.readValue(reader, Catalog.class);
        return (catalog == null) ? new Catalog(null) : catalog;
    }
}
