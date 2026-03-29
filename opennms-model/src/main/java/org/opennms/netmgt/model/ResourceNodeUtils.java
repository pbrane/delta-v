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
package org.opennms.netmgt.model;

import org.springframework.orm.ObjectRetrievalFailureException;

/**
 * Entity-aware resource utilities that depend on JPA model types.
 * Extracted from ResourceTypeUtils to keep model-api persistence-free.
 */
public abstract class ResourceNodeUtils {

    /**
     * Convenience method for retrieving the OnmsNode entity from
     * an abstract resource.
     *
     * @throws ObjectRetrievalFailureException on failure
     */
    public static OnmsNode getNodeFromResource(OnmsResource resource) {
        if (resource == null) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, "Resource must be non-null.");
        }

        final OnmsEntity entity = resource.getEntity();
        if (entity == null) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, "Resource entity must be non-null: " + resource);
        }

        if (!(entity instanceof OnmsNode)) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, "Resource entity must be an instance of OnmsNode: " + resource);
        }

        return (OnmsNode) entity;
    }

    /**
     * Convenience method for retrieving the OnmsNode entity from
     * an abstract resource's ancestor.
     *
     * @throws ObjectRetrievalFailureException on failure
     */
    public static OnmsNode getNodeFromResourceRoot(final OnmsResource resource) {
        OnmsResource res = resource;
        while (res != null && res.getParent() != null) {
            res = res.getParent();
        }

        if (res == null) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, "Resource must be non-null.");
        }

        final OnmsEntity entity = res.getEntity();
        if (entity == null) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, "Resource entity must be non-null: " + resource);
        }

        if (!(entity instanceof OnmsNode)) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, "Resource entity must be an instance of OnmsNode: " + resource);
        }

        return (OnmsNode) entity;
    }
}
