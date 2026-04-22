/*
 * Copyright (C) 1999-2024 The OpenNMS Group, Inc.
 * Copyright (C) 2026 BeaconStrategists, Inc. (Modifications)
 *
 * This file is part of OpenNMS(R) / Delta-V.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
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
package org.opennms.netmgt.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.rrd.RrdRepository;

class ResourceTypeUtilsTest {

    @Test
    void getResourcePathWithRepository_prependsRepositoryBaseDirName() {
        final RrdRepository repository = new RrdRepository();
        repository.setRrdBaseDir(new File("/var/opennms/rrd/snmp"));
        final ResourcePath resource = ResourcePath.get("fs", "Example", "node-1");

        final ResourcePath result = ResourceTypeUtils.getResourcePathWithRepository(repository, resource);

        assertThat(result.elements()).containsExactly("snmp", "fs", "Example", "node-1");
    }

    @Test
    void getResourcePathWithRepository_worksWithResponseRepository() {
        final RrdRepository repository = new RrdRepository();
        repository.setRrdBaseDir(new File("share/rrd/response"));
        final ResourcePath resource = ResourcePath.get("127.0.0.1");

        final ResourcePath result = ResourceTypeUtils.getResourcePathWithRepository(repository, resource);

        assertThat(result.elements()).containsExactly("response", "127.0.0.1");
    }
}
