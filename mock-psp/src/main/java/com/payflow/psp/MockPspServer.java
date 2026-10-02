package com.payflow.psp;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;

/** Standalone mock PSP: {@code java -jar mock-psp.jar [port]} (default 9095). */
public final class MockPspServer {

    public static Server start(MockPsp psp, int port) throws IOException {
        return NettyServerBuilder.forPort(port)
                // fault injection sleeps on the handler thread; virtual threads make that free
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(psp.provider)
                .addService(psp.admin)
                .build()
                .start();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9095;
        Server server = start(new MockPsp(), port);
        System.out.println("mock-psp listening on " + server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdownNow));
        server.awaitTermination();
    }
}
