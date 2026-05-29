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
package org.deltav.netmgt.alarmd.boot.persister;

import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import org.deltav.alarms.proto.AlarmState;
import org.deltav.netmgt.alarmd.boot.cache.ReductionCache;
import org.opennms.netmgt.alarmd.AlarmPersister;
import org.opennms.netmgt.alarmd.AlarmPersisterImpl;
import org.opennms.netmgt.dao.api.AlarmEntityNotifier;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsSeverity;
import org.opennms.netmgt.xml.event.AlarmData;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Logmsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delta-V replacement for horizon's {@link AlarmPersisterImpl} (spec §3.2 / §3.2.1).
 *
 * <p>In {@code kafka-only} mode (default at v1.3.0 GA), reduction lookups hit
 * the in-memory {@link ReductionCache} (populated by the bootstrap-replay of
 * the compacted {@code deltav-alarms-state-change} Kafka topic, then live-tailed
 * by alarmd's own publishes). The resulting {@link OnmsAlarm} is then fanned
 * out via {@link AlarmEntityNotifier} — the existing chain
 * {@code AlarmEntityNotifier → AlarmEntityListener(s) → AlarmLifecycleListenerManager
 * → AlarmLifecycleListener(s)} drives the Kafka publisher (Track 1) without
 * any PostgreSQL write occurring.</p>
 *
 * <p>In {@code dual-write} mode (one-release escape hatch), the wrapped horizon
 * {@link AlarmPersisterImpl} is invoked verbatim, restoring the legacy
 * PG-write + AlarmEntityNotifier chain. This mode exists so that operators
 * can fall back to PostgreSQL-backed alarms for a release if a defect is
 * discovered in the Kafka-only path post-GA.</p>
 *
 * <h2>Reduction-key derivation</h2>
 * <p>The canonical horizon path is {@code event.getAlarmData().getReductionKey()}.
 * Horizon's {@link AlarmPersisterImpl} private helper {@code
 * checkEventSanityAndDoWeProcess(Event)} short-circuits on a null {@code AlarmData},
 * so the same constraint applies here: an event with no {@link AlarmData} or
 * blank reduction key is dropped (returns {@code null}). This matches horizon's
 * observable behavior without depending on private helpers.</p>
 */
public class DeltavAlarmPersister implements AlarmPersister {

    private static final Logger LOG = LoggerFactory.getLogger(DeltavAlarmPersister.class);

    private final AlarmPersisterImpl dualWriteDelegate;
    private final ReductionCache reductionCache;
    private final AlarmEntityNotifier notifier;
    private final AlarmdPersistenceProperties properties;

    public DeltavAlarmPersister(AlarmPersisterImpl dualWriteDelegate,
                                ReductionCache reductionCache,
                                AlarmEntityNotifier notifier,
                                AlarmdPersistenceProperties properties) {
        this.dualWriteDelegate = Objects.requireNonNull(dualWriteDelegate, "dualWriteDelegate");
        this.reductionCache = Objects.requireNonNull(reductionCache, "reductionCache");
        this.notifier = Objects.requireNonNull(notifier, "notifier");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    @Override
    public OnmsAlarm persist(Event event) {
        if (properties.isDualWrite()) {
            LOG.trace("dual-write mode: delegating to horizon AlarmPersisterImpl for uei={}",
                    event != null ? event.getUei() : null);
            return dualWriteDelegate.persist(event);
        }

        // kafka-only path (default)
        if (event == null) {
            return null;
        }
        String reductionKey = deriveReductionKey(event);
        if (reductionKey == null || reductionKey.isBlank()) {
            LOG.debug("dropping event uei={} — no reduction_key derivable from AlarmData",
                    event.getUei());
            return null;
        }

        Optional<AlarmState> cached = reductionCache.get(reductionKey);
        if (cached.isPresent()) {
            OnmsAlarm reduced = reduceFromCachedState(cached.get(), event);
            LOG.trace("kafka-only: reduced existing alarm rk={} counter={}",
                    reductionKey, reduced.getCounter());
            notifier.didUpdateAlarmWithReducedEvent(reduced);
            return reduced;
        }

        OnmsAlarm fresh = createNewAlarmFromEvent(event, reductionKey);
        LOG.trace("kafka-only: created new alarm rk={} uei={}", reductionKey, event.getUei());
        notifier.didCreateAlarm(fresh);
        return fresh;
    }

    /**
     * The canonical horizon reduction-key path is
     * {@code event.getAlarmData().getReductionKey()}. {@link AlarmPersisterImpl}
     * gates its private {@code addOrReduceEventAsAlarm} on the same field.
     */
    private static String deriveReductionKey(Event event) {
        AlarmData alarmData = event.getAlarmData();
        if (alarmData == null) {
            return null;
        }
        return alarmData.getReductionKey();
    }

    /**
     * Build an {@link OnmsAlarm} mirroring the cached state, incrementing the
     * counter and overlaying severity/log/desc from the incoming event (falling
     * back to cached values when the event omits them).
     */
    private static OnmsAlarm reduceFromCachedState(AlarmState cached, Event event) {
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setId(cached.getAlarmId() != 0 ? cached.getAlarmId() : null);
        alarm.setReductionKey(cached.getReductionKey());
        alarm.setUei(cached.getUei());
        alarm.setCounter(cached.getCount() + 1);
        alarm.setFirstEventTime(epochToDate(cached.getFirstEventTimeMs()));
        alarm.setLastEventTime(new Date());

        OnmsSeverity severity = severityFromEvent(event);
        if (severity == null) {
            severity = severityFromProto(cached.getSeverity());
        }
        alarm.setSeverity(severity);

        String logmsg = logmsgContent(event);
        alarm.setLogMsg(logmsg != null ? logmsg : nullIfEmpty(cached.getLogMessage()));

        String descr = event.getDescr();
        alarm.setDescription(descr != null ? descr : nullIfEmpty(cached.getDescription()));

        return alarm;
    }

    /**
     * Build a fresh {@link OnmsAlarm} for a never-before-seen reduction key.
     * Counter starts at 1; firstEventTime = lastEventTime = now.
     */
    private static OnmsAlarm createNewAlarmFromEvent(Event event, String reductionKey) {
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setReductionKey(reductionKey);
        alarm.setUei(event.getUei());
        alarm.setCounter(1);
        Date now = new Date();
        alarm.setFirstEventTime(now);
        alarm.setLastEventTime(now);
        OnmsSeverity severity = severityFromEvent(event);
        alarm.setSeverity(severity != null ? severity : OnmsSeverity.INDETERMINATE);
        alarm.setLogMsg(logmsgContent(event));
        alarm.setDescription(event.getDescr());
        return alarm;
    }

    private static String logmsgContent(Event event) {
        Logmsg logmsg = event.getLogmsg();
        return logmsg != null ? logmsg.getContent() : null;
    }

    private static OnmsSeverity severityFromEvent(Event event) {
        String label = event.getSeverity();
        if (label == null || label.isBlank()) {
            return null;
        }
        return OnmsSeverity.get(label);
    }

    private static OnmsSeverity severityFromProto(AlarmState.Severity sev) {
        if (sev == null) {
            return OnmsSeverity.INDETERMINATE;
        }
        switch (sev) {
            case CLEARED:       return OnmsSeverity.CLEARED;
            case NORMAL:        return OnmsSeverity.NORMAL;
            case WARNING:       return OnmsSeverity.WARNING;
            case MINOR:         return OnmsSeverity.MINOR;
            case MAJOR:         return OnmsSeverity.MAJOR;
            case CRITICAL:      return OnmsSeverity.CRITICAL;
            case INDETERMINATE:
            case UNRECOGNIZED:
            default:            return OnmsSeverity.INDETERMINATE;
        }
    }

    private static Date epochToDate(long ms) {
        return ms != 0 ? new Date(ms) : null;
    }

    private static String nullIfEmpty(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }
}
