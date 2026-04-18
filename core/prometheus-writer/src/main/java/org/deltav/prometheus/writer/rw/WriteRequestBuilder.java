/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.deltav.prometheus.writer.translate.PromSample;
import org.springframework.stereotype.Component;
import prometheus.prompb.Label;
import prometheus.prompb.Sample;
import prometheus.prompb.TimeSeries;
import prometheus.prompb.WriteRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class WriteRequestBuilder {
    public WriteRequest build(List<PromSample> samples) {
        Map<SeriesKey, List<Sample>> bySeries = new LinkedHashMap<>();
        Map<SeriesKey, List<Label>> seriesLabels = new HashMap<>();
        for (PromSample s : samples) {
            Map<String, String> withName = new LinkedHashMap<>();
            withName.put("__name__", s.name());
            withName.putAll(s.labels());
            SeriesKey key = SeriesKey.from(withName);
            bySeries.computeIfAbsent(key, k -> new ArrayList<>())
                    .add(Sample.newBuilder().setValue(s.value()).setTimestamp(s.timestampMs()).build());
            seriesLabels.computeIfAbsent(key, k -> toLabels(withName));
        }
        WriteRequest.Builder req = WriteRequest.newBuilder();
        for (Map.Entry<SeriesKey, List<Sample>> e : bySeries.entrySet()) {
            req.addTimeseries(TimeSeries.newBuilder()
                    .addAllLabels(seriesLabels.get(e.getKey()))
                    .addAllSamples(e.getValue()));
        }
        return req.build();
    }

    private static List<Label> toLabels(Map<String, String> m) {
        List<Label> out = new ArrayList<>(m.size());
        m.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> out.add(Label.newBuilder().setName(e.getKey()).setValue(e.getValue()).build()));
        return out;
    }

    private record SeriesKey(String canonical) {
        static SeriesKey from(Map<String, String> labels) {
            StringBuilder sb = new StringBuilder();
            labels.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .forEach(e -> sb.append(e.getKey()).append('=').append(e.getValue()).append('|'));
            return new SeriesKey(sb.toString());
        }
    }
}
