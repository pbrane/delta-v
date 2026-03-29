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
package org.opennms.minion.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SpringMinionIdentityTest {

    @Test
    void exposesIdAndLocation() {
        MinionProperties properties = new MinionProperties();
        properties.setId("minion-01");
        properties.setLocation("NYC");

        SpringMinionIdentity identity = new SpringMinionIdentity(properties);

        assertThat(identity.getId()).isEqualTo("minion-01");
        assertThat(identity.getLocation()).isEqualTo("NYC");
        assertThat(identity.getType()).isEqualTo("Minion");
    }
}
