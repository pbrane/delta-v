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
package org.opennms.netmgt.model.jakarta.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;

import org.deltav.core.daemon.common.AbstractDaoJpa;
import org.opennms.netmgt.dao.api.AcknowledgmentDao;
import org.opennms.netmgt.dao.api.AlarmEntityNotifier;
import org.opennms.netmgt.model.AckAction;
import org.opennms.netmgt.model.AckType;
import org.opennms.netmgt.model.Acknowledgeable;
import org.opennms.netmgt.model.OnmsAcknowledgment;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsSeverity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA implementation of {@link AcknowledgmentDao}.
 *
 * <p>Mirrors {@code AcknowledgmentDaoHibernate}'s behavior using Jakarta JPA
 * + EntityManager instead of Hibernate 3.x's {@code HibernateTemplate}.
 *
 * <p>Required by Drools alarm-lifecycle automation: {@code DefaultAlarmService}
 * calls {@link #processAck(OnmsAcknowledgment)} on rule-driven ack/unack/clear,
 * and {@code DroolsAlarmContext} reads via {@link #findLatestAcks(Date)} +
 * {@link #findLatestAckForRefId(Integer)} to seed and refresh KieSession facts.
 *
 * <p>Currently only handles {@link AckType#ALARM} ackables — the legacy
 * {@code AckType.NOTIFICATION} path requires {@code OnmsNotification} which
 * is not part of delta-v's Jakarta entity set.
 */
@Repository
@Transactional
public class AcknowledgmentDaoJpa extends AbstractDaoJpa<OnmsAcknowledgment, Integer> implements AcknowledgmentDao {

    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgmentDaoJpa.class);

    @Autowired
    private AlarmEntityNotifier alarmEntityNotifier;

    public AcknowledgmentDaoJpa() {
        super(OnmsAcknowledgment.class);
    }

    @Override
    public void updateAckable(Acknowledgeable ackable) {
        entityManager().merge(ackable);
    }

    @Override
    public List<Acknowledgeable> findAcknowledgables(final OnmsAcknowledgment ack) {
        List<Acknowledgeable> ackables = new ArrayList<>();
        if (ack == null || ack.getAckType() == null) {
            return ackables;
        }
        if (ack.getAckType().equals(AckType.ALARM)) {
            final OnmsAlarm alarm = findAlarm(ack);
            if (alarm != null && alarm.getAckId() != null) {
                ackables.add(alarm);
            }
        }
        return ackables;
    }

    private OnmsAlarm findAlarm(final OnmsAcknowledgment ack) {
        if (ack == null || ack.getRefId() == null) {
            return null;
        }
        try {
            return entityManager().find(OnmsAlarm.class, ack.getRefId());
        } catch (Exception e) {
            LOG.warn("Unable to find alarm with ID {}", ack.getRefId(), e);
            return null;
        }
    }

    @Transactional
    @Override
    public void processAcks(Collection<OnmsAcknowledgment> acks) {
        LOG.info("processAcks: Processing {} acknowledgements...", acks.size());
        for (OnmsAcknowledgment ack : acks) {
            processAck(ack);
        }
    }

    @Transactional
    @Override
    public void processAck(OnmsAcknowledgment ack) {
        LOG.info("processAck: Searching DB for acknowledgables for ack: {}", ack);
        List<Acknowledgeable> ackables = findAcknowledgables(ack);

        if (ackables == null || ackables.isEmpty()) {
            LOG.debug("processAck: No acknowledgables found.");
            throw new IllegalStateException("No acknowledgables in the database for ack: " + ack);
        }

        LOG.debug("processAck: Found {}. Acknowledging...", ackables.size());

        Iterator<Acknowledgeable> it = ackables.iterator();
        while (it.hasNext()) {
            try {
                Acknowledgeable ackable = it.next();
                final boolean isAlarm = ackable instanceof OnmsAlarm;
                Consumer<OnmsAlarm> callback = null;

                switch (ack.getAckAction()) {
                    case ACKNOWLEDGE:
                        LOG.debug("processAck: Acknowledging ackable: {}...", ackable);
                        if (isAlarm) {
                            final String ackUser = ackable.getAckUser();
                            final Date ackTime = ackable.getAckTime();
                            callback = alarm -> alarmEntityNotifier.didAcknowledgeAlarm(alarm, ackUser, ackTime);
                        }
                        ackable.acknowledge(ack.getAckUser());
                        break;
                    case UNACKNOWLEDGE:
                        LOG.debug("processAck: Unacknowledging ackable: {}...", ackable);
                        if (isAlarm) {
                            final String ackUser = ackable.getAckUser();
                            final Date ackTime = ackable.getAckTime();
                            callback = alarm -> alarmEntityNotifier.didUnacknowledgeAlarm(alarm, ackUser, ackTime);
                        }
                        ackable.unacknowledge(ack.getAckUser());
                        break;
                    case CLEAR:
                        LOG.debug("processAck: Clearing ackable: {}...", ackable);
                        if (isAlarm) {
                            ((OnmsAlarm) ackable).getRelatedAlarms().forEach(this::clearRelatedAlarm);
                            final OnmsSeverity previousSeverity = ackable.getSeverity();
                            callback = alarm -> alarmEntityNotifier.didUpdateAlarmSeverity(alarm, previousSeverity);
                        }
                        ackable.clear(ack.getAckUser());
                        break;
                    case ESCALATE:
                        LOG.debug("processAck: Escalating ackable: {}...", ackable);
                        if (isAlarm) {
                            final OnmsSeverity previousSeverity = ackable.getSeverity();
                            callback = alarm -> alarmEntityNotifier.didUpdateAlarmSeverity(alarm, previousSeverity);
                        }
                        ackable.escalate(ack.getAckUser());
                        break;
                    default:
                        break;
                }

                updateAckable(ackable);
                save(ack);
                flush();

                if (callback != null) {
                    callback.accept((OnmsAlarm) ackable);
                }
            } catch (Throwable t) {
                LOG.error("processAck: exception while processing: {}", ack, t);
            }
        }
        LOG.info("processAck: Found and processed acknowledgables for the acknowledgement: {}", ack);
    }

    private void clearRelatedAlarm(OnmsAlarm alarm) {
        OnmsAcknowledgment clear = new OnmsAcknowledgment(alarm);
        clear.setAckAction(AckAction.CLEAR);
        processAck(clear);
    }

    @Override
    @Transactional
    public List<OnmsAcknowledgment> findLatestAcks(Date from) {
        // Mirrors AcknowledgmentDaoHibernate's correlated-subquery shape:
        // for each refId, pick the row with the latest ackTime; on tie, pick max id.
        final String jpql =
                "SELECT acks FROM OnmsAcknowledgment acks " +
                "WHERE acks.ackTime = (" +
                "    SELECT MAX(filteredAcks.ackTime) FROM OnmsAcknowledgment filteredAcks " +
                "    WHERE filteredAcks.refId = acks.refId) " +
                "AND acks.id = (" +
                "    SELECT MAX(filteredAcks.id) FROM OnmsAcknowledgment filteredAcks " +
                "    WHERE filteredAcks.refId = acks.refId) " +
                "AND acks.ackTime >= :minAckTime";
        TypedQuery<OnmsAcknowledgment> query = entityManager()
                .createQuery(jpql, OnmsAcknowledgment.class);
        query.setParameter("minAckTime", from);
        return query.getResultList();
    }

    @Override
    @Transactional
    public Optional<OnmsAcknowledgment> findLatestAckForRefId(Integer refId) {
        if (refId == null) {
            return Optional.empty();
        }
        final String jpql =
                "SELECT acks FROM OnmsAcknowledgment acks " +
                "WHERE acks.refId = :refId " +
                "ORDER BY acks.ackTime DESC, acks.id DESC";
        TypedQuery<OnmsAcknowledgment> query = entityManager()
                .createQuery(jpql, OnmsAcknowledgment.class);
        query.setParameter("refId", refId);
        query.setMaxResults(1);
        try {
            List<OnmsAcknowledgment> results = query.getResultList();
            return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
        } catch (NoResultException e) {
            return Optional.empty();
        }
    }
}
