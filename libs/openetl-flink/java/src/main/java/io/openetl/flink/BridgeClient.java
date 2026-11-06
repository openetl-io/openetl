package io.openetl.flink;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/** Channels to the runner's bridge, and its calls' framing. */
final class BridgeClient {
    static final String PROTOCOL = "v1";
    static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> PROTOCOL_HEADER =
            Metadata.Key.of("openetl-bridge-protocol", Metadata.ASCII_STRING_MARSHALLER);
    static final int MAX_MESSAGE = 256 << 20;
    /** The prefix of a channel's threads' names. */
    static final String THREADS = "openetl-bridge";
    private static final long CLOSE_WAIT_MS = 5_000;

    private BridgeClient() {}

    /**
     * A channel that trusts only the certificate with the config's fingerprint, whatever its name.
     * It has threads of its own, which {@link #close} stops: Flink closes a job's classloader once
     * its task ends, and gRPC's shared threads, released a moment after a channel shuts down, then
     * fail to load their classes.
     */
    static ManagedChannel open(BridgeConfig c) {
        SslContext ssl;
        try {
            ssl =
                    GrpcSslContexts.configure(SslContextBuilder.forClient(), SslProvider.JDK)
                            .trustManager(new PinnedTrustManager(c.fingerprint))
                            .build();
        } catch (SSLException e) {
            throw new IllegalStateException("openetl: TLS for the bridge", e);
        }
        NioEventLoopGroup group = new NioEventLoopGroup(1, new DefaultThreadFactory(THREADS + "-io", true));
        AtomicInteger n = new AtomicInteger();
        ExecutorService executor =
                Executors.newCachedThreadPool(
                        r -> {
                            Thread t = new Thread(r, THREADS + "-" + n.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        });
        ManagedChannel ch =
                NettyChannelBuilder.forAddress(c.host, c.port)
                        .eventLoopGroup(group)
                        .channelType(NioSocketChannel.class)
                        .executor(executor)
                        // Name resolution's.
                        .offloadExecutor(executor)
                        .sslContext(ssl)
                        .maxInboundMessageSize(MAX_MESSAGE)
                        .intercept(new Credentials(c.token))
                        .build();
        return new OwnedChannel(ch, group, executor);
    }

    /**
     * Shuts ch down, and waits for its threads to stop, even if the thread is interrupted. It loads
     * no class of ours, since Flink may have closed the job's classloader by the time an operator
     * coordinator closes.
     */
    static void close(ManagedChannel ch) {
        if (ch == null) {
            return;
        }
        boolean interrupted = Thread.interrupted();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLOSE_WAIT_MS);
        ch.shutdownNow();
        interrupted |= await(ch, deadline);
        if (ch instanceof OwnedChannel) {
            OwnedChannel o = (OwnedChannel) ch;
            o.group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
            o.executor.shutdownNow();
            interrupted |= await(o.group, deadline);
            interrupted |= await(o.executor, deadline);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Waits for ch until deadline at most, and says whether the thread was interrupted. */
    private static boolean await(ManagedChannel ch, long deadline) {
        boolean interrupted = false;
        while (true) {
            try {
                ch.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                return interrupted;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
    }

    private static boolean await(ExecutorService e, long deadline) {
        boolean interrupted = false;
        while (true) {
            try {
                e.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                return interrupted;
            } catch (InterruptedException x) {
                interrupted = true;
            }
        }
    }

    /** A channel and the threads only it uses. */
    private static final class OwnedChannel extends ManagedChannel {
        private final ManagedChannel ch;
        final NioEventLoopGroup group;
        final ExecutorService executor;

        OwnedChannel(ManagedChannel ch, NioEventLoopGroup group, ExecutorService executor) {
            this.ch = ch;
            this.group = group;
            this.executor = executor;
        }

        @Override
        public ManagedChannel shutdown() {
            ch.shutdown();
            return this;
        }

        @Override
        public boolean isShutdown() {
            return ch.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return ch.isTerminated();
        }

        @Override
        public ManagedChannel shutdownNow() {
            ch.shutdownNow();
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return ch.awaitTermination(timeout, unit);
        }

        @Override
        public <Q, R> ClientCall<Q, R> newCall(MethodDescriptor<Q, R> method, CallOptions options) {
            return ch.newCall(method, options);
        }

        @Override
        public String authority() {
            return ch.authority();
        }
    }

    static IOException failure(String what, Throwable t) {
        if (t instanceof StatusRuntimeException) {
            Status s = ((StatusRuntimeException) t).getStatus();
            String d = s.getDescription() == null ? "" : ": " + s.getDescription();
            return new IOException("openetl: " + what + ": " + s.getCode() + d, t);
        }
        return new IOException("openetl: " + what + ": " + t, t);
    }

    private static final class Credentials implements ClientInterceptor {
        private final String token;

        Credentials(String token) {
            this.token = token;
        }

        @Override
        public <Q, R> ClientCall<Q, R> interceptCall(
                MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<Q, R>(
                    next.newCall(method, options)) {
                @Override
                public void start(Listener<R> listener, Metadata headers) {
                    headers.put(AUTHORIZATION, "Bearer " + token);
                    headers.put(PROTOCOL_HEADER, PROTOCOL);
                    super.start(listener, headers);
                }
            };
        }
    }

    static final class PinnedTrustManager extends X509ExtendedTrustManager {
        private final String fingerprint;

        PinnedTrustManager(String fingerprint) {
            this.fingerprint = fingerprint;
        }

        static String fingerprint(X509Certificate cert) throws CertificateEncodingException {
            try {
                byte[] d = MessageDigest.getInstance("SHA-256").digest(cert.getEncoded());
                StringBuilder b = new StringBuilder(64);
                for (byte x : d) {
                    b.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
                }
                return b.toString();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            if (chain == null || chain.length == 0) {
                throw new CertificateException("openetl: the bridge sent no certificate");
            }
            String got = fingerprint(chain[0]);
            if (!got.equals(fingerprint)) {
                throw new CertificateException(
                        "openetl: the bridge's certificate has fingerprint " + got + ", not " + fingerprint);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
                throws CertificateException {
            checkServerTrusted(chain, authType);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
                throws CertificateException {
            checkServerTrusted(chain, authType);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            throw new CertificateException("openetl: not a server");
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
                throws CertificateException {
            throw new CertificateException("openetl: not a server");
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
                throws CertificateException {
            throw new CertificateException("openetl: not a server");
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    /**
     * A bidirectional call whose server answers each request that needs it, in order. Sends wait
     * for flow control; both waits end on an interrupt, which cancels the call.
     */
    static final class AckedStream<Q, R> {
        private final String what;
        private final BlockingQueue<Object> responses = new LinkedBlockingQueue<>();
        private final Object ready = new Object();
        private volatile ClientCallStreamObserver<Q> requests;
        private volatile Throwable error;
        private volatile boolean closing;

        private AckedStream(String what) {
            this.what = what;
        }

        /** Starts the call; onEnd gets how it ended unless close ended it. */
        static <Q, R> AckedStream<Q, R> open(
                String what, Function<StreamObserver<R>, StreamObserver<Q>> start, Consumer<Throwable> onEnd) {
            AckedStream<Q, R> s = new AckedStream<>(what);
            start.apply(
                    new ClientResponseObserver<Q, R>() {
                        @Override
                        public void beforeStart(ClientCallStreamObserver<Q> r) {
                            s.requests = r;
                            r.setOnReadyHandler(s::wake);
                        }

                        @Override
                        public void onNext(R value) {
                            s.responses.add(value);
                        }

                        @Override
                        public void onError(Throwable t) {
                            s.end(t, onEnd);
                        }

                        @Override
                        public void onCompleted() {
                            s.end(
                                    Status.UNAVAILABLE.withDescription("the runner ended the call").asRuntimeException(),
                                    onEnd);
                        }
                    });
            return s;
        }

        private void end(Throwable t, Consumer<Throwable> onEnd) {
            error = t;
            responses.add(t);
            wake();
            if (!closing && onEnd != null) {
                onEnd.accept(t);
            }
        }

        private void wake() {
            synchronized (ready) {
                ready.notifyAll();
            }
        }

        Throwable error() {
            return error;
        }

        void send(Q request) throws IOException, InterruptedException {
            synchronized (ready) {
                while (error == null && !requests.isReady()) {
                    try {
                        ready.wait(100);
                    } catch (InterruptedException e) {
                        cancel("interrupted");
                        throw e;
                    }
                }
            }
            if (error != null) {
                throw failure(what, error);
            }
            requests.onNext(request);
        }

        @SuppressWarnings("unchecked")
        R await() throws IOException, InterruptedException {
            Object o;
            try {
                o = responses.take();
            } catch (InterruptedException e) {
                cancel("interrupted");
                throw e;
            }
            if (o instanceof Throwable) {
                responses.add(o);
                throw failure(what, (Throwable) o);
            }
            return (R) o;
        }

        void cancel(String why) {
            closing = true;
            ClientCallStreamObserver<Q> r = requests;
            if (r != null && error == null) {
                r.cancel(why, null);
            }
        }

        /** Half-closes the call; nothing the runner says after is reported. */
        void close() {
            closing = true;
            if (error == null) {
                try {
                    requests.onCompleted();
                } catch (IllegalStateException ignored) {
                    // Already cancelled.
                }
            }
        }
    }
}
