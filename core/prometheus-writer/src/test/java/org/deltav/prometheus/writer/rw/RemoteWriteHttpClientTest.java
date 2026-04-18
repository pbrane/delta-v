/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;
import org.xerial.snappy.Snappy;
import prometheus.prompb.WriteRequest;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteWriteHttpClientTest {
    private MockWebServer server;

    @BeforeEach void setUp() throws Exception { server = new MockWebServer(); server.start(); }
    @AfterEach  void tearDown() throws Exception { server.shutdown(); }

    private RemoteWriteHttpClient client(PrometheusWriterProperties.AuthType auth, String tok, String user, String pass, Map<String,String> extra) {
        PrometheusWriterProperties props = new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite(server.url("/api/v1/write").toString(),
                        new PrometheusWriterProperties.Auth(auth, tok, user, pass), extra),
                null, null, null, null, null);
        return new RemoteWriteHttpClient(RestClient.builder(), props);
    }

    @Test
    void post_200_returns_200_with_correct_headers_and_snappy_body() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        int status = client(PrometheusWriterProperties.AuthType.NONE, null, null, null, Map.of())
                .post(WriteRequest.newBuilder().build());
        assertThat(status).isEqualTo(200);
        RecordedRequest req = server.takeRequest(2, TimeUnit.SECONDS);
        assertThat(req).isNotNull();
        assertThat(req.getHeader("Content-Type")).isEqualTo("application/x-protobuf");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        assertThat(req.getHeader("X-Prometheus-Remote-Write-Version")).isEqualTo("0.1.0");
        // Body must be Snappy-decompressible (round-trip to verify framing)
        byte[] body = req.getBody().readByteArray();
        byte[] uncompressed = Snappy.uncompress(body);
        assertThat(uncompressed).isNotNull();  // would throw on bad framing
    }

    @Test
    void post_with_bearer_adds_authorization_header() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        client(PrometheusWriterProperties.AuthType.BEARER, "abc", null, null, Map.of())
                .post(WriteRequest.newBuilder().build());
        RecordedRequest req = server.takeRequest(2, TimeUnit.SECONDS);
        assertThat(req.getHeader("Authorization")).isEqualTo("Bearer abc");
    }

    @Test
    void post_with_basic_adds_basic_auth_header() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        client(PrometheusWriterProperties.AuthType.BASIC, null, "u", "p", Map.of())
                .post(WriteRequest.newBuilder().build());
        RecordedRequest req = server.takeRequest(2, TimeUnit.SECONDS);
        assertThat(req.getHeader("Authorization")).isEqualTo("Basic dTpw");
    }

    @Test
    void post_with_extra_headers_adds_them() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        client(PrometheusWriterProperties.AuthType.NONE, null, null, null, Map.of("X-Scope-OrgID", "tenant-1"))
                .post(WriteRequest.newBuilder().build());
        RecordedRequest req = server.takeRequest(2, TimeUnit.SECONDS);
        assertThat(req.getHeader("X-Scope-OrgID")).isEqualTo("tenant-1");
    }

    @Test
    void post_500_throws_server_error() {
        server.enqueue(new MockResponse().setResponseCode(500));
        assertThatThrownBy(() ->
                client(PrometheusWriterProperties.AuthType.NONE, null, null, null, Map.of())
                        .post(WriteRequest.newBuilder().build()))
                .isInstanceOf(HttpServerErrorException.class);
    }

    @Test
    void post_400_throws_client_error() {
        server.enqueue(new MockResponse().setResponseCode(400));
        assertThatThrownBy(() ->
                client(PrometheusWriterProperties.AuthType.NONE, null, null, null, Map.of())
                        .post(WriteRequest.newBuilder().build()))
                .isInstanceOf(HttpClientErrorException.class);
    }
}
