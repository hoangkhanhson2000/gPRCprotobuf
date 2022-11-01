package com.example.grpcprotobuf.goodbye;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class GoodbyeServer {
    private static final Logger logger = Logger.getLogger(com.example.grpcprotobuf.goodbye.GoodbyeServer.class.getName());

    private Server server;

    private void start() throws IOException {

        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new GoodbyeImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {}) {
            @Override
            public void run() {

                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    com.example.grpcprotobuf.goodbye.GoodbyeServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        final com.example.grpcprotobuf.goodbye.GoodbyeServer server = new com.example.grpcprotobuf.goodbye.GoodbyeServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class GoodbyeImpl extends GoodbyeGrpc.GoodbyeImplBase {

        @Override
        public void sayGoodbye(GoodbyeRequest req, StreamObserver<GoodbyeReply> responseObserver) {
            GoodbyeReply reply = GoodbyeReply.newBuilder().setMessage("Sayonara " + req.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}


