package org.deltav.gateway.rpc;

import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.RpcRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MinionStreamPoolTest {

    @Test
    void emptyPool_returnsNullForUnknownLocation() {
        MinionStreamPool pool = new MinionStreamPool();
        assertThat(pool.pickStream("Default")).isNull();
    }

    @Test
    void registeredStream_returnsStreamForItsLocation() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> observer = mock(StreamObserver.class);
        pool.register("Default", "minion-A", observer);

        assertThat(pool.pickStream("Default")).isSameAs(observer);
    }

    @Test
    void multipleStreamsInSameLocation_roundRobin() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> a = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> b = mock(StreamObserver.class);
        pool.register("Default", "minion-A", a);
        pool.register("Default", "minion-B", b);

        assertThat(pool.pickStream("Default")).isSameAs(a);
        assertThat(pool.pickStream("Default")).isSameAs(b);
        assertThat(pool.pickStream("Default")).isSameAs(a);
    }

    @Test
    void unregisteredStream_noLongerSelectable() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> a = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> b = mock(StreamObserver.class);
        pool.register("Default", "minion-A", a);
        pool.register("Default", "minion-B", b);
        pool.unregister("Default", "minion-A");

        assertThat(pool.pickStream("Default")).isSameAs(b);
        assertThat(pool.pickStream("Default")).isSameAs(b);
    }

    @Test
    void emptyAfterAllUnregister_returnsNull() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> a = mock(StreamObserver.class);
        pool.register("Default", "minion-A", a);
        pool.unregister("Default", "minion-A");

        assertThat(pool.pickStream("Default")).isNull();
    }
}
