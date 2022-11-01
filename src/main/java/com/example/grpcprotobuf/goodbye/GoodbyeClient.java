package com.example.grpcprotobuf.goodbye;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GoodbyeClient {
    private static final Logger logger = Logger.getLogger(com.example.grpcprotobuf.goodbye.GoodbyeClient.class.getName());

    private final GoodbyeGrpc.GoodbyeBlockingStub blockingStub;

    public GoodbyeClient(Channel channel) {

        blockingStub = GoodbyeGrpc.newBlockingStub(channel);
    }

    public void goodbye(String name) {
        logger.info("Say goodbye " + name + ", see you again");
        GoodbyeRequest request = GoodbyeRequest.newBuilder().setName(name).build();
        GoodbyeReply response;
        try {
            response = blockingStub.sayGoodbye(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Say Goodbye: " + response.getMessage());
    }

    public static void main(String[] args) throws Exception {
        String user = "Hoang Son";
        String target = "localhost:50051";

        if (args.length > 0) {
            if ("--help".equals(args[0])) {
                System.err.println("Usage: [name [target]]");
                System.err.println();
                System.err.println("  name    The name you wish to be say goodbye by. Defaults to " + user);
                System.err.println("  target  The server to connect to. Defaults to " + target);
                System.exit(1);
            }
            user = args[0];
        }
        if (args.length > 1) {
            target = args[1];
        }


        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)

                .usePlaintext()
                .build();
        try {
            com.example.grpcprotobuf.goodbye.GoodbyeClient client = new com.example.grpcprotobuf.goodbye.GoodbyeClient(channel);
            client.goodbye(user);
        } finally {
            channel.shutdownNow().awaitTermination(4, TimeUnit.SECONDS);
        }
    }
}

