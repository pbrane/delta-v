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
package org.opennms.netmgt.config.api;

import static org.opennms.core.utils.InetAddressUtils.addr;
import static org.opennms.core.utils.InetAddressUtils.toIpAddrBytes;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opennms.netmgt.config.collectd.CollectdConfiguration;
import org.opennms.netmgt.config.collectd.Collector;
import org.opennms.netmgt.config.collectd.Package;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link CollectdConfigFactory} that works with
 * a pre-loaded {@link CollectdConfiguration} model object and an injected
 * {@link FilterDao}.
 *
 * <p>This class replaces the need for daemon-boot modules to depend on
 * the opennms-config singleton factory. The configuration model can be
 * loaded via Jackson XmlMapper or any other means, then passed to this
 * constructor along with a FilterDao for filter evaluation.</p>
 *
 * <p>The {@code reload()} and {@code saveCurrent()} operations are not
 * supported in this implementation — daemon-boot modules load config once
 * at startup and do not write changes back to disk.</p>
 */
public class DefaultCollectdConfigFactory implements CollectdConfigFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultCollectdConfigFactory.class);

    private final CollectdConfiguration localConfig;
    private final FilterDao filterDao;
    private final Object mutex = new Object();

    private List<Package> extendedPackages = Collections.emptyList();
    private List<Collector> extendedCollectors = Collections.emptyList();
    private List<Package> mergedPackages;
    private List<Collector> mergedCollectors;

    public DefaultCollectdConfigFactory(final CollectdConfiguration config, final FilterDao filterDao) {
        this.localConfig = config;
        this.filterDao = filterDao;
        rebuildMergedData();
    }

    private void rebuildMergedData() {
        synchronized (mutex) {
            mergedPackages = new ArrayList<>(localConfig.getPackages());
            mergedPackages.addAll(extendedPackages);
            mergedCollectors = new ArrayList<>(localConfig.getCollectors());
            mergedCollectors.addAll(extendedCollectors);
        }
    }

    @Override
    public void reload() throws IOException {
        LOG.warn("reload() is not supported in DefaultCollectdConfigFactory — config is loaded once at startup");
    }

    @Override
    public void saveCurrent() throws IOException {
        LOG.warn("saveCurrent() is not supported in DefaultCollectdConfigFactory — config changes are not persisted");
    }

    @Override
    public CollectdConfiguration getLocalCollectdConfig() {
        return localConfig;
    }

    @Override
    public Integer getThreads() {
        return localConfig.getThreads();
    }

    @Override
    public boolean packageExists(String name) {
        synchronized (mutex) {
            for (final Package pkg : mergedPackages) {
                if (pkg.getName().equals(name)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public Package getPackage(final String name) {
        synchronized (mutex) {
            for (Package pkg : mergedPackages) {
                if (pkg.getName().equals(name)) {
                    return pkg;
                }
            }
            return null;
        }
    }

    @Override
    public List<Package> getPackages() {
        return mergedPackages;
    }

    @Override
    public List<Collector> getCollectors() {
        return mergedCollectors;
    }

    @Override
    public boolean domainExists(final String name) {
        synchronized (mutex) {
            for (Package pkg : mergedPackages) {
                if (pkg.getIfAliasDomain() != null && pkg.getIfAliasDomain().equals(name)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public boolean isServiceCollectionEnabled(final OnmsMonitoredService service) {
        return isServiceCollectionEnabled(service.getIpInterface(), service.getServiceName());
    }

    @Override
    public boolean isServiceCollectionEnabled(final OnmsIpInterface iface, final String svcName) {
        for (Package wpkg : mergedPackages) {
            if (interfaceInPackage(iface, wpkg)) {
                if (wpkg.serviceInPackageAndEnabled(svcName)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isServiceCollectionEnabled(final String ipAddr, final String svcName) {
        synchronized (mutex) {
            for (Package wpkg : mergedPackages) {
                if (interfaceInPackage(ipAddr, wpkg)) {
                    if (wpkg.serviceInPackageAndEnabled(svcName)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    @Override
    public boolean interfaceInFilter(String iface, Package pkg) {
        String filter = pkg.getFilter().getContent();
        if (iface == null) return false;
        final InetAddress ifaceAddress = addr(iface);

        LOG.debug("interfaceInFilter: package is {}. filter rules are {}", pkg.getName(), filter);
        try {
            final List<InetAddress> ipList = filterDao.getActiveIPAddressList(filter);
            boolean filterPassed = ipList.contains(ifaceAddress);
            if (!filterPassed) {
                LOG.debug("interfaceInFilter: Interface {} passed filter for package {}?: false", iface, pkg.getName());
            }
            return filterPassed;
        } catch (Throwable t) {
            LOG.error("interfaceInFilter: Failed to map package: {} to an IP List with filter \"{}\"",
                    pkg.getName(), pkg.getFilter().getContent(), t);
            return false;
        }
    }

    @Override
    public boolean interfaceInPackage(final String iface, Package pkg) {
        boolean filterPassed = interfaceInFilter(iface, pkg);
        if (!filterPassed) {
            return false;
        }

        byte[] addr = toIpAddrBytes(iface);
        boolean hasRangeInclude = pkg.hasIncludeRange(iface);
        boolean hasSpecific = pkg.hasSpecific(addr);
        hasSpecific = pkg.hasSpecificUrl(iface, hasSpecific);
        boolean hasRangeExclude = pkg.hasExcludeRange(iface);

        boolean packagePassed = hasSpecific || (hasRangeInclude && !hasRangeExclude);
        if (packagePassed) {
            LOG.info("interfaceInPackage: Interface {} passed filter and specific/range for package {}?: {}",
                    iface, pkg.getName(), true);
        } else {
            LOG.debug("interfaceInPackage: Interface {} passed filter and specific/range for package {}?: {}",
                    iface, pkg.getName(), false);
        }
        return packagePassed;
    }

    @Override
    public boolean interfaceInPackage(final OnmsIpInterface iface, Package pkg) {
        return interfaceInPackage(iface.getIpAddressAsString(), pkg);
    }

    @Override
    public void setExternalData(final List<Package> packages, final List<Collector> collectors) {
        synchronized (mutex) {
            this.extendedPackages = packages;
            this.extendedCollectors = collectors;
            rebuildMergedData();
        }
    }
}
