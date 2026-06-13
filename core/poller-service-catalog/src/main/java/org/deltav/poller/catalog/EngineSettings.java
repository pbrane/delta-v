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

/**
 * Engine-tuning scalars that the frozen poller manager answers from the synthetic
 * {@code PollerConfiguration} root (amended D3), bound from {@code application.yml} by
 * daemon-boot. These are plumbing, not monitoring semantics, so they live here rather than
 * in the catalog file (FR1b's three-way split).
 *
 * <p>This is a plain record — no Spring, no JAXB — so the library stays framework-free; the
 * {@link CatalogTranslator} maps these onto the JAXB root attributes.
 *
 * <p>Note: {@code nextOutageIdSql} is deliberately absent — it is dead in delta-v's engine
 * (Open Verification Item #3: zero call sites; outage ids come from JPA), so the translator
 * leaves the JAXB default in place rather than carrying a value.
 *
 * @param threads                    poller thread-pool size
 * @param asyncPollingEngineEnabled  whether the async polling engine is enabled (default false)
 * @param maxConcurrentAsyncPolls    cap on concurrent async polls when async is enabled
 */
public record EngineSettings(int threads, boolean asyncPollingEngineEnabled, int maxConcurrentAsyncPolls) {

    /** Conservative defaults (sync engine) for tests and minimal deployments. */
    public static EngineSettings defaults() {
        return new EngineSettings(30, false, 100);
    }
}
