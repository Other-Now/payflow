package com.payflow.loadtest;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.payflow.proto.Empty;
import com.payflow.proto.FaultConfig;
import com.payflow.proto.Ledger;
import com.payflow.proto.PspAdminGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** The whole system on one box: DynamoDB Local in-process, three services as child JVMs. */
final class Cluster implements AutoCloseable {

    static final int DYNAMO = 8000, ELIGIBILITY = 9091, PSP = 9095, HTTP = 8180;

    private final Path root;
    private final Path logs;
    private final List<String> paymentArgs;
    private final List<Process> children = new ArrayList<>();
    private DynamoDBProxyServer dynamo;
    private Process payment;
    private int paymentStarts;

    final ManagedChannel pspChannel;
    final ManagedChannel eligibilityChannel;
    final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(2)).build();

    Cluster(Path root, Path logs, List<String> paymentArgs) {
        this.root = root;
        this.logs = logs;
        this.paymentArgs = paymentArgs;
        this.pspChannel = ManagedChannelBuilder.forAddress("localhost", PSP).usePlaintext().build();
        this.eligibilityChannel = ManagedChannelBuilder.forAddress("localhost", ELIGIBILITY).usePlaintext().build();
        Runtime.getRuntime().addShutdownHook(new Thread(this::killAll));
    }

    void start() throws Exception {
        Files.createDirectories(logs);
        dynamo = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", Integer.toString(DYNAMO)});
        dynamo.start();
        spawn("mock-psp", root.resolve("mock-psp/target/payflow-mock-psp-0.1.0-exec.jar"), List.of(Integer.toString(PSP)));
        spawn("eligibility", root.resolve("eligibility-service/target/eligibility-service.jar"),
                List.of("--payflow.grpc.port=" + ELIGIBILITY));
        awaitPort(PSP);
        awaitPort(ELIGIBILITY);
        startPayment();
    }

    void startPayment() throws Exception {
        List<String> args = new ArrayList<>(List.of(
                "--server.port=" + HTTP,
                "--payflow.dynamo.endpoint=http://localhost:" + DYNAMO,
                "--payflow.psp-target=localhost:" + PSP,
                "--payflow.eligibility-target=localhost:" + ELIGIBILITY));
        args.addAll(paymentArgs);
        payment = spawn("payment-" + (++paymentStarts), root.resolve("payment-service/target/payment-service.jar"), args);
        awaitHealthy();
    }

    /** SIGKILL on Linux, TerminateProcess on Windows: no shutdown hooks, no graceful drain. */
    void killPayment() throws InterruptedException {
        payment.destroyForcibly();
        payment.waitFor();
    }

    void setFaults(FaultConfig f) {
        PspAdminGrpc.newBlockingStub(pspChannel).setFaults(f);
    }

    Ledger ledger() {
        return PspAdminGrpc.newBlockingStub(pspChannel).dumpLedger(Empty.getDefaultInstance());
    }

    HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + HTTP + path))
                .timeout(Duration.ofSeconds(30)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private Process spawn(String name, Path jar, List<String> args) throws IOException {
        if (!Files.exists(jar)) throw new IllegalStateException(jar + " missing: run mvn package first");
        List<String> cmd = new ArrayList<>(List.of(javaBin(), "-Xmx256m", "-XX:+UseSerialGC", "-jar", jar.toString()));
        cmd.addAll(args);
        Process p = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(logs.resolve(name + ".log").toFile()))
                .start();
        children.add(p);
        return p;
    }

    private void awaitHealthy() throws Exception {
        long deadline = System.currentTimeMillis() + 240_000; // generous: a memory-starved laptop boots Spring slowly
        while (System.currentTimeMillis() < deadline) {
            try {
                if (get("/actuator/health").statusCode() == 200) return;
            } catch (IOException ignored) {
                // not up yet
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("payment-service did not become healthy");
    }

    private static void awaitPort(int port) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 90_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 500);
                return;
            } catch (IOException e) {
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("port " + port + " never opened");
    }

    private static String javaBin() {
        return ProcessHandle.current().info().command()
                .orElse(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    }

    private void killAll() {
        children.forEach(Process::destroyForcibly);
    }

    @Override
    public void close() throws Exception {
        pspChannel.shutdownNow();
        eligibilityChannel.shutdownNow();
        killAll();
        if (dynamo != null) dynamo.stop();
    }
}
