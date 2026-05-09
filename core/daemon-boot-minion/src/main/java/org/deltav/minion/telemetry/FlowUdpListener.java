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
package org.deltav.minion.telemetry;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.deltav.minion.telemetry.proto.TelemetryProtos.TelemetryMessage;
import org.deltav.minion.telemetry.proto.TelemetryProtos.TelemetryMessageLog;
import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * Netty-based UDP listener that receives flow protocol datagrams on a
 * single port, detects the protocol via {@link FlowProtocol#detect}, and
 * dispatches each datagram to a per-protocol {@link AsyncDispatcher}
 * targeting the {@code DeltaV.Sink.Telemetry-{protocol}} Kafka topic.
 *
 * <p>Owns its own single-thread {@link NioEventLoopGroup}. The existing
 * Minion Trap listener uses horizon's {@code TrapListener} (internal
 * socket management) and the Syslog listener uses
 * {@code SyslogReceiverJavaNetImpl} (plain {@code DatagramSocket}), so
 * there is no pre-existing Netty group to share.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>{@link #start()} binds the UDP port and starts the event loop</li>
 *   <li>{@link #stop()} closes the channel and shuts the event loop down</li>
 * </ul>
 */
public class FlowUdpListener {

    private static final Logger LOG = LoggerFactory.getLogger(FlowUdpListener.class);

    /** Minimum interval between "unknown protocol" warnings, in nanoseconds. */
    private static final long WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final int port;
    private final String bindAddress;
    private final Map<FlowProtocol, AsyncDispatcher<FlowTelemetryMessage>> dispatchers;
    private final String location;
    private final String systemId;

    private final AtomicLong lastUnknownWarnNanos = new AtomicLong(0);

    private EventLoopGroup eventLoopGroup;
    private Channel channel;

    public FlowUdpListener(int port,
                           String bindAddress,
                           Map<FlowProtocol, AsyncDispatcher<FlowTelemetryMessage>> dispatchers,
                           String location,
                           String systemId) {
        this.port = port;
        this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
        this.dispatchers = Objects.requireNonNull(dispatchers, "dispatchers");
        this.location = Objects.requireNonNull(location, "location");
        this.systemId = Objects.requireNonNull(systemId, "systemId");
    }

    public synchronized void start() throws InterruptedException {
        if (channel != null) {
            return;
        }
        eventLoopGroup = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, false)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel ch) {
                            ch.pipeline().addLast(new DatagramHandler());
                        }
                    });

            InetSocketAddress local = "*".equals(bindAddress)
                    ? new InetSocketAddress(port)
                    : new InetSocketAddress(bindAddress, port);
            channel = bootstrap.bind(local).sync().channel();
            LOG.info("Flow telemetry listener started on {}:{} (protocols: Netflow-5, Netflow-9, IPFIX, sFlow)",
                    bindAddress, getBoundPort());
        } catch (InterruptedException | RuntimeException e) {
            // Clean up the event loop group so a subsequent start() doesn't leak it
            // and Spring's lifecycle doesn't get stuck with an orphaned group.
            eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
            eventLoopGroup = null;
            throw e;
        }
    }

    public synchronized void stop() throws InterruptedException {
        if (channel != null) {
            channel.close().sync();
            channel = null;
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
            eventLoopGroup = null;
        }
        for (AsyncDispatcher<?> d : dispatchers.values()) {
            try {
                d.close();
            } catch (Exception e) {
                LOG.warn("Failed to close dispatcher: {}", e.getMessage());
            }
        }
        LOG.info("Flow telemetry listener stopped");
    }

    /**
     * Returns the actual bound port. Useful for tests that bind to port 0
     * and need to know the ephemeral port the kernel chose.
     */
    public int getBoundPort() {
        if (channel == null) {
            return -1;
        }
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    private void handleDatagram(ByteBuf buf, InetSocketAddress sender) {
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);

        FlowProtocol protocol = FlowProtocol.detect(payload);
        if (protocol == null) {
            maybeWarnUnknownProtocol(payload.length, sender);
            return;
        }

        TelemetryMessage message = TelemetryMessage.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setBytes(ByteString.copyFrom(payload))
                .build();

        TelemetryMessageLog log = TelemetryMessageLog.newBuilder()
                .setLocation(location)
                .setSystemId(systemId)
                .setSourceAddress(sender.getAddress().getHostAddress())
                .setSourcePort(sender.getPort())
                .addMessage(message)
                .build();

        AsyncDispatcher<FlowTelemetryMessage> dispatcher = dispatchers.get(protocol);
        if (dispatcher == null) {
            // Defensive: should never happen because the config class wires
            // a dispatcher for every FlowProtocol.
            LOG.warn("No dispatcher registered for {}; dropping datagram", protocol);
            return;
        }
        dispatcher.send(new FlowTelemetryMessage(log));
    }

    private void maybeWarnUnknownProtocol(int length, InetSocketAddress sender) {
        long now = System.nanoTime();
        long last = lastUnknownWarnNanos.get();
        if (now - last >= WARN_INTERVAL_NANOS
                && lastUnknownWarnNanos.compareAndSet(last, now)) {
            LOG.warn("Dropping {}-byte datagram from {} with unrecognized protocol version header",
                    length, sender);
        }
    }

    private class DatagramHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
            handleDatagram(msg.content(), msg.sender());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("Exception in flow UDP handler: {}", cause.getMessage(), cause);
        }
    }
}
