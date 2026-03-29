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

import java.util.Collections;
import java.util.List;

import org.opennms.core.criteria.Criteria;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.model.OnmsDistPoller;

/**
 * A database-free {@link DistPollerDao} for Minion deployments.
 *
 * <p>Minion does not have a local PostgreSQL instance, so this implementation
 * derives the poller identity solely from {@link SpringMinionIdentity}. All
 * mutating operations throw {@link UnsupportedOperationException}.</p>
 */
public class MinionDistPollerDao implements DistPollerDao {

    private final OnmsDistPoller self;

    public MinionDistPollerDao(SpringMinionIdentity identity) {
        this.self = new OnmsDistPoller();
        self.setId(identity.getId());
        self.setLabel(identity.getId());
        self.setLocation(identity.getLocation());
        self.setType("Minion");
    }

    @Override
    public OnmsDistPoller whoami() {
        return self;
    }

    // ------------------------------------------------------------------ //
    // Read-only / no-op operations                                         //
    // ------------------------------------------------------------------ //

    @Override
    public void lock() {
    }

    @Override
    public void initialize(Object obj) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void clear() {
    }

    @Override
    public int countAll() {
        return 1;
    }

    @Override
    public List<OnmsDistPoller> findAll() {
        return Collections.singletonList(self);
    }

    @Override
    public List<OnmsDistPoller> findMatching(Criteria criteria) {
        return Collections.singletonList(self);
    }

    @Override
    public int countMatching(Criteria criteria) {
        return 1;
    }

    @Override
    public OnmsDistPoller get(String id) {
        return self.getId().equals(id) ? self : null;
    }

    @Override
    public OnmsDistPoller load(String id) {
        return get(id);
    }

    // ------------------------------------------------------------------ //
    // Mutating operations — not supported on Minion                       //
    // ------------------------------------------------------------------ //

    @Override
    public void delete(OnmsDistPoller entity) {
        throw new UnsupportedOperationException("MinionDistPollerDao is read-only");
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException("MinionDistPollerDao is read-only");
    }

    @Override
    public String save(OnmsDistPoller entity) {
        throw new UnsupportedOperationException("MinionDistPollerDao is read-only");
    }

    @Override
    public void saveOrUpdate(OnmsDistPoller entity) {
        throw new UnsupportedOperationException("MinionDistPollerDao is read-only");
    }

    @Override
    public void update(OnmsDistPoller entity) {
        throw new UnsupportedOperationException("MinionDistPollerDao is read-only");
    }
}
