/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.deltav.prometheus.writer.translate.PromSample;
import org.junit.jupiter.api.Test;
import prometheus.prompb.Label;
import prometheus.prompb.TimeSeries;
import prometheus.prompb.WriteRequest;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WriteRequestBuilderTest {

    private final WriteRequestBuilder builder = new WriteRequestBuilder();

    @Test
    void builds_one_timeseries_per_unique_label_set() {
        List<PromSample> samples = List.of(
                new PromSample("a", Map.of("x", "1"), 1.0d, 1_000L),
                new PromSample("a", Map.of("x", "1"), 2.0d, 2_000L),
                new PromSample("b", Map.of("y", "1"), 3.0d, 3_000L)
        );

        WriteRequest wr = builder.build(samples);

        assertThat(wr.getTimeseriesCount()).isEqualTo(2);
    }

    @Test
    void multiple_samples_same_series_grouped() {
        List<PromSample> samples = List.of(
                new PromSample("foo", Map.of("k", "v"), 1.0d, 1_000L),
                new PromSample("foo", Map.of("k", "v"), 2.0d, 2_000L),
                new PromSample("foo", Map.of("k", "v"), 3.0d, 3_000L)
        );

        WriteRequest wr = builder.build(samples);

        assertThat(wr.getTimeseriesCount()).isEqualTo(1);
        TimeSeries ts = wr.getTimeseries(0);
        assertThat(ts.getSamplesCount()).isEqualTo(3);
        assertThat(ts.getSamples(0).getValue()).isEqualTo(1.0d);
        assertThat(ts.getSamples(1).getValue()).isEqualTo(2.0d);
        assertThat(ts.getSamples(2).getValue()).isEqualTo(3.0d);
    }

    @Test
    void metric_name_goes_to___name___label() {
        List<PromSample> samples = List.of(
                new PromSample("foo", Map.of(), 1.0d, 1_000L)
        );

        WriteRequest wr = builder.build(samples);

        assertThat(wr.getTimeseriesCount()).isEqualTo(1);
        TimeSeries ts = wr.getTimeseries(0);
        assertThat(ts.getLabelsList())
                .extracting(Label::getName, Label::getValue)
                .contains(org.assertj.core.groups.Tuple.tuple("__name__", "foo"));
    }

    @Test
    void labels_sorted_alphabetically_in_output() {
        List<PromSample> samples = List.of(
                new PromSample("metric", Map.of("zzz", "z", "aaa", "a", "mmm", "m"), 1.0d, 1_000L)
        );

        WriteRequest wr = builder.build(samples);

        TimeSeries ts = wr.getTimeseries(0);
        List<String> labelNames = ts.getLabelsList().stream().map(Label::getName).toList();
        // __name__ sorts alphabetically before all letters because '_' (0x5F) < 'a' (0x61)
        assertThat(labelNames).containsExactly("__name__", "aaa", "mmm", "zzz");
    }
}
