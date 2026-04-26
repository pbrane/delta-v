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
package org.deltav.gateway.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.deltav.minion.grpc.v1.Heartbeat;
import org.deltav.minion.grpc.v1.HeartbeatAck;
import org.deltav.minion.grpc.v1.HeartbeatServiceGrpc;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.grpc.server.lifecycle.GrpcServerLifecycle;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end IT: real-startup minion-gateway daemon, real Kafka broker
 * (Testcontainers), gRPC client publishes one Heartbeat with identity
 * metadata and asserts the matching record on
 * {@code OpenNMS.Sink.Heartbeat}.
 *
 * <p>Uses the {@link ApplicationContextInitializer} pattern (rather than
 * {@code @DynamicPropertySource}) to start the Kafka container before the
 * Spring Boot context loads — Spring Boot 4 / JUnit 5 ordering otherwise
 * bootstraps the context before {@code @Testcontainers} runs
 * {@code beforeAll}. Pattern matches the existing Layer 4 IT in
 * {@code core/daemon-boot-collectd/.../TimeseriesKafkaBrokerIT.java}.
 *
 * <p>Bind gRPC to ephemeral port (0) and read the resolved port back via
 * {@link GrpcServerLifecycle#getPort()}; injecting the configured
 * {@code spring.grpc.server.port} would just return {@code 0}.
 */
@SpringBootTest(
    properties = {
        "spring.grpc.server.port=0",
        "server.port=0",
        "spring.main.banner-mode=off"
    })
@ContextConfiguration(initializers = HeartbeatGrpcServiceIT.KafkaInitializer.class)
class HeartbeatGrpcServiceIT {

    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0"))
            .withStartupTimeout(Duration.ofSeconds(120));

    static class KafkaInitializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext ctx) {
            if (!KAFKA.isRunning()) {
                KAFKA.start();
            }
            String brokers = KAFKA.getBootstrapServers();
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(ctx,
                "spring.kafka.bootstrap-servers=" + brokers);
        }
    }

    @Autowired
    GrpcServerLifecycle grpcServerLifecycle;

    @Test
    void heartbeatPublishedViaGrpc_landsOnKafkaTopic() throws Exception {
        int grpcPort = grpcServerLifecycle.getPort();
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", grpcPort)
            .usePlaintext().build();

        Metadata headers = new Metadata();
        headers.put(MinionIdentityServerInterceptor.MINION_ID_HEADER, "minion-IT");
        headers.put(MinionIdentityServerInterceptor.MINION_LOCATION_HEADER, "loc-IT");

        var stub = HeartbeatServiceGrpc.newStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        CountDownLatch ackLatch = new CountDownLatch(1);
        StreamObserver<HeartbeatAck> obs = new StreamObserver<>() {
            public void onNext(HeartbeatAck a) { ackLatch.countDown(); }
            public void onError(Throwable t) {}
            public void onCompleted() {}
        };
        StreamObserver<Heartbeat> sender = stub.publish(obs);
        sender.onNext(Heartbeat.newBuilder()
            .setMinionId("minion-IT").setLocation("loc-IT")
            .setSentAt(Timestamp.newBuilder().setSeconds(1745625600L).build())
            .setVersion("1.2.0-rc1").build());

        assertThat(ackLatch.await(10, TimeUnit.SECONDS)).isTrue();

        Properties cp = new Properties();
        cp.put("bootstrap.servers", KAFKA.getBootstrapServers());
        cp.put("group.id", "it-consumer-" + System.nanoTime());
        cp.put("auto.offset.reset", "earliest");
        cp.put("key.deserializer", StringDeserializer.class);
        cp.put("value.deserializer", ByteArrayDeserializer.class);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(cp)) {
            consumer.subscribe(Collections.singletonList("OpenNMS.Sink.Heartbeat"));
            List<ConsumerRecord<String, byte[]>> all = new java.util.ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (all.isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(all::add);
            }
            assertThat(all).hasSize(1);
            assertThat(all.get(0).key()).isEqualTo("loc-IT@minion-IT");
            assertThat(new String(all.get(0).value())).contains("<id>minion-IT</id>");
        }

        sender.onCompleted();
        channel.shutdown();
    }
}
