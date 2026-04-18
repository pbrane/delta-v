/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NameSanitizerTest {
    private final NameSanitizer s = new NameSanitizer();

    @Test void mib2_hyphen_to_underscore()      { assertThat(s.sanitize("mib2-interface-errors")).isEqualTo("mib2_interface_errors"); }
    @Test void camelcase_to_lowercase()         { assertThat(s.sanitize("ifInDiscards")).isEqualTo("ifindiscards"); }
    @Test void mixed_case_with_hyphens()        { assertThat(s.sanitize("mib2-X-interfaces")).isEqualTo("mib2_x_interfaces"); }
    @Test void simple_camelcase()               { assertThat(s.sanitize("hrStorage")).isEqualTo("hrstorage"); }
    @Test void leading_digit_prefixed()         { assertThat(s.sanitize("1abc")).isEqualTo("_1abc"); }
    @Test void consecutive_hyphens_collapsed()  { assertThat(s.sanitize("foo--bar")).isEqualTo("foo_bar"); }
    @Test void mixed_separators_collapsed()     { assertThat(s.sanitize("foo..bar::baz")).isEqualTo("foo_bar_baz"); }
    @Test void consecutive_underscores_collapsed() { assertThat(s.sanitize("foo___bar")).isEqualTo("foo_bar"); }
    @Test void empty_returns_empty()            { assertThat(s.sanitize("")).isEqualTo(""); }
    @Test void already_clean_unchanged()        { assertThat(s.sanitize("foo")).isEqualTo("foo"); }

    @Test
    void detect_collisions_finds_dupes() {
        Map<String, List<String>> collisions = s.detectCollisions(List.of("foo-bar", "foo_bar"));
        assertThat(collisions).containsKey("foo_bar");
        assertThat(collisions.get("foo_bar")).containsExactlyInAnyOrder("foo-bar", "foo_bar");
    }
}
