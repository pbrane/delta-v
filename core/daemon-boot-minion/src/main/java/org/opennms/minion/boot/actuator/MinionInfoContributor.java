/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.minion.boot.actuator;

import java.util.Map;

import org.opennms.distributed.core.api.MinionIdentity;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.stereotype.Component;

/**
 * Contributes Minion identity information to the {@code /actuator/info} endpoint.
 *
 * <p>Replaces the Karaf shell command {@code opennms:id}.</p>
 */
@Component
public class MinionInfoContributor implements InfoContributor {

    private final MinionIdentity identity;

    public MinionInfoContributor(MinionIdentity identity) {
        this.identity = identity;
    }

    @Override
    public void contribute(Info.Builder builder) {
        builder.withDetail("minion", Map.of(
            "id", identity.getId(),
            "location", identity.getLocation(),
            "type", identity.getType()
        ));
    }
}
