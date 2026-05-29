/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deltav.alarms.materializer.config.MaterializerProperties;
import org.deltav.alarms.materializer.metrics.MaterializerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled retention task. For each {@link RetentionRule}, selects matching
 * {@code reductionkey}s from PG and produces a tombstone for each. Does NOT
 * delete PG rows — the materializer's own consumer reads the tombstone and
 * issues the DELETE (spec §4.5.3). This enforces the "consumer is the only
 * PG writer" invariant at the code level.
 */
@Component
public class RetentionEngine {

    private static final Logger LOG = LoggerFactory.getLogger(RetentionEngine.class);

    private final MaterializerProperties props;
    private final JdbcTemplate jdbc;
    private final Producer<String, byte[]> producer;
    private final PredicateEvaluator evaluator;
    private final List<RetentionRule> rules;
    private final MeterRegistry meterRegistry;

    public RetentionEngine(MaterializerProperties props,
                           JdbcTemplate jdbc,
                           Producer<String, byte[]> producer,
                           PredicateEvaluator evaluator,
                           List<RetentionRule> rules,
                           MeterRegistry meterRegistry) {
        this.props = props;
        this.jdbc = jdbc;
        this.producer = producer;
        this.evaluator = evaluator;
        this.rules = rules;
        this.meterRegistry = meterRegistry;
    }

    /** Runs all rules once. Public for tests; scheduled invocation goes via {@link #scheduledRun()}. */
    public void runOnce() {
        meterRegistry.counter(MaterializerMetrics.RETENTION_EVALS).increment();
        for (RetentionRule rule : rules) {
            PredicateEvaluator.Compiled compiled = evaluator.compile(rule);
            List<String> matches = jdbc.queryForList(
                    "SELECT reductionkey FROM alarms WHERE " + compiled.whereClause(),
                    String.class,
                    compiled.params().toArray());

            if (matches.isEmpty()) {
                continue;
            }
            LOG.info("Retention rule '{}' matched {} alarm(s) — producing tombstones", rule.id(), matches.size());
            for (String rk : matches) {
                producer.send(new ProducerRecord<>(props.getAlarmsTopic(), rk, null));
                meterRegistry.counter(MaterializerMetrics.RETENTION_TOMBSTONES).increment();
                meterRegistry.counter(MaterializerMetrics.RETENTION_DELETES,
                        MaterializerMetrics.TAG_RULE_ID, rule.id()).increment();
            }
        }
    }

    @Scheduled(fixedDelayString = "${deltav.alarms-materializer.retention.cadence:PT1M}",
               initialDelayString = "${deltav.alarms-materializer.retention.initial-delay:PT30S}")
    public void scheduledRun() {
        try {
            runOnce();
        } catch (Exception e) {
            LOG.error("Retention scan failed; will retry on next cadence", e);
        }
    }
}
