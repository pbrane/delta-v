/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.xerial.snappy.Snappy;
import prometheus.prompb.WriteRequest;

import java.util.Map;
import java.util.Optional;

@Component
public class RemoteWriteHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteWriteHttpClient.class);
    private final RestClient client;
    private final PrometheusWriterProperties props;

    public RemoteWriteHttpClient(RestClient.Builder builder, PrometheusWriterProperties props) {
        this.props = props;
        this.client = builder.baseUrl(props.remoteWrite().url()).build();
    }

    /** POSTs the WriteRequest to the configured RW URL. Returns HTTP status code on success.
     *  Throws on 4xx/5xx (use RemoteWriteRetryPolicy in Task 12 to classify exceptions). */
    public int post(WriteRequest request) throws Exception {
        byte[] compressed = Snappy.compress(request.toByteArray());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("application/x-protobuf"));
        headers.set("Content-Encoding", "snappy");
        headers.set("X-Prometheus-Remote-Write-Version", "0.1.0");
        String version = Optional.ofNullable(getClass().getPackage().getImplementationVersion()).orElse("dev");
        headers.set("User-Agent", "deltav-prometheus-writer/" + version);

        var auth = props.remoteWrite().auth();
        switch (auth.type()) {
            case BEARER -> headers.setBearerAuth(auth.bearerToken());
            case BASIC  -> headers.setBasicAuth(auth.basicUsername(), auth.basicPassword());
            case NONE   -> { /* no-op */ }
        }
        for (Map.Entry<String, String> e : props.remoteWrite().headers().entrySet()) {
            headers.add(e.getKey(), e.getValue());
        }

        ResponseEntity<Void> resp = client.post()
                .uri(props.remoteWrite().url())
                .headers(h -> h.addAll(headers))
                .body(compressed)
                .retrieve()
                .toBodilessEntity();
        return resp.getStatusCode().value();
    }
}
