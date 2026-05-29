/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.upsert;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Resolves a service name (e.g. "ICMP") to its {@code service.serviceid} PK.
 * Service rows are effectively immutable for the lifetime of the daemon, so
 * results — including misses ({@link Optional#empty()}) — cache forever.
 */
@Component
public class ServiceTypeResolver {

    private static final String SQL =
            "SELECT serviceid FROM service WHERE servicename = ?";

    private final JdbcTemplate jdbc;
    private final ConcurrentHashMap<String, Optional<Integer>> cache = new ConcurrentHashMap<>();

    public ServiceTypeResolver(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Integer resolve(String serviceName) {
        if (serviceName == null || serviceName.isEmpty()) {
            return null;
        }
        return cache.computeIfAbsent(serviceName, name -> {
            try {
                return Optional.ofNullable(jdbc.queryForObject(SQL, Integer.class, name));
            } catch (EmptyResultDataAccessException miss) {
                return Optional.empty();
            }
        }).orElse(null);
    }
}
