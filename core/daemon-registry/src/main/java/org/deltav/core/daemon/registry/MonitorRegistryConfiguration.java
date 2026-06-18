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
package org.deltav.core.daemon.registry;

import java.util.List;

import org.opennms.netmgt.poller.ServiceMonitorRegistry;
import org.opennms.netmgt.poller.monitors.BgpSessionMonitor;
import org.opennms.netmgt.poller.monitors.DNSResolutionMonitor;
import org.opennms.netmgt.poller.monitors.DnsMonitor;
import org.opennms.netmgt.poller.monitors.FtpMonitor;
import org.opennms.netmgt.poller.monitors.HttpMonitor;
import org.opennms.netmgt.poller.monitors.HttpsMonitor;
import org.opennms.netmgt.poller.monitors.IcmpMonitor;
import org.opennms.netmgt.poller.monitors.ImapMonitor;
import org.opennms.netmgt.poller.monitors.MinaSshMonitor;
import org.opennms.netmgt.poller.monitors.NtpMonitor;
import org.opennms.netmgt.poller.monitors.PageSequenceMonitor;
import org.opennms.netmgt.poller.monitors.PassiveServiceMonitor;
import org.opennms.netmgt.poller.monitors.Pop3Monitor;
import org.opennms.netmgt.poller.monitors.PtpMonitor;
import org.opennms.netmgt.poller.monitors.SSLCertMonitor;
import org.opennms.netmgt.poller.monitors.SmtpMonitor;
import org.opennms.netmgt.poller.monitors.SnmpMonitor;
import org.opennms.netmgt.poller.monitors.SshMonitor;
import org.opennms.netmgt.poller.monitors.StrafePingMonitor;
import org.opennms.netmgt.poller.monitors.TcpMonitor;
import org.opennms.netmgt.poller.monitors.WebMonitor;
import org.opennms.netmgt.poller.monitors.Win32ServiceMonitor;
import org.opennms.protocols.radius.monitor.RadiusAuthMonitor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the service monitors that Delta-V supports.
 * Import this configuration from any daemon boot config that needs a
 * {@link ServiceMonitorRegistry}.
 */
@Configuration
public class MonitorRegistryConfiguration {

    @Bean
    public ServiceMonitorRegistry serviceMonitorRegistry() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return new LocalServiceMonitorRegistry(List.of(
            new IcmpMonitor(),
            new SnmpMonitor(),
            new TcpMonitor(),
            new HttpMonitor(),
            new HttpsMonitor(),
            new DnsMonitor(),
            new SshMonitor(),
            new SSLCertMonitor(),
            new PageSequenceMonitor(),
            new BgpSessionMonitor(),
            new DNSResolutionMonitor(),
            new MinaSshMonitor(),
            new NtpMonitor(),
            new StrafePingMonitor(),
            new WebMonitor(),
            new PassiveServiceMonitor(),
            new RadiusAuthMonitor(),
            // Catalog service types whose monitors live in the core monitors jar but were
            // previously unregistered (false-DOWN if provisioned). All no-arg constructible,
            // no new dependencies. See poller-services.yaml: SMTP/FTP/IMAP/POP3/PTP/Windows-Task-Scheduler.
            new SmtpMonitor(),
            new FtpMonitor(),
            new ImapMonitor(),
            new Pop3Monitor(),
            new PtpMonitor(),
            new Win32ServiceMonitor()
        ));
    }
}
