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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Dictionary;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.osgi.service.cm.Configuration;

class SpringConfigurationAdminTest {

    @Test
    void getConfigurationReturnsSinkProperties() throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");

        SpringConfigurationAdmin admin = new SpringConfigurationAdmin(kafkaProps);
        Configuration config = admin.getConfiguration("org.opennms.core.ipc.sink.kafka");

        Dictionary<String, Object> dict = config.getProperties();
        assertThat(dict.get("bootstrap.servers")).isEqualTo("kafka:9092");
    }

    @Test
    void anyPidReturnsSameProperties() throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

        SpringConfigurationAdmin admin = new SpringConfigurationAdmin(kafkaProps);

        assertThat(admin.getConfiguration("any.pid").getProperties().get("bootstrap.servers"))
            .isEqualTo("localhost:9092");
        assertThat(admin.getConfiguration("other.pid", "location").getProperties().get("bootstrap.servers"))
            .isEqualTo("localhost:9092");
    }

    @Test
    void getPidReturnsSpringKafka() throws Exception {
        SpringConfigurationAdmin admin = new SpringConfigurationAdmin(new Properties());
        assertThat(admin.getConfiguration("any.pid").getPid()).isEqualTo("spring-kafka");
    }

    @Test
    void createFactoryConfigurationThrowsUnsupported() {
        SpringConfigurationAdmin admin = new SpringConfigurationAdmin(new Properties());

        assertThatThrownBy(() -> admin.createFactoryConfiguration("any.factory"))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> admin.createFactoryConfiguration("any.factory", "location"))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> admin.getFactoryConfiguration("factory", "name"))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> admin.getFactoryConfiguration("factory", "name", "location"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void listConfigurationsReturnsSingleElement() throws Exception {
        SpringConfigurationAdmin admin = new SpringConfigurationAdmin(new Properties());
        Configuration[] result = admin.listConfigurations(null);
        assertThat(result).hasSize(1);
    }

    @Test
    void multiplePropertiesAreAllPopulated() throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("security.protocol", "SASL_SSL");
        kafkaProps.setProperty("sasl.mechanism", "PLAIN");

        SpringConfigurationAdmin admin = new SpringConfigurationAdmin(kafkaProps);
        Dictionary<String, Object> dict = admin.getConfiguration("any.pid").getProperties();

        assertThat(dict.get("bootstrap.servers")).isEqualTo("kafka:9092");
        assertThat(dict.get("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(dict.get("sasl.mechanism")).isEqualTo("PLAIN");
    }
}
