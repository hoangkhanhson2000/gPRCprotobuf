package com.example.grpcprotobuf.manualflowcontrol;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ManualFlowControlClient {
    private static final Logger logger =
        Logger.getLogger(ManualFlowControlClient.class.getName());

  public static void main(String[] args) throws InterruptedException {
    final CountDownLatch done = new CountDownLatch(1);
      ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .usePlaintext()
        .build();
    StreamingGreeterGrpc.StreamingGreeterStub stub = StreamingGreeterGrpc.newStub(channel);
    ClientResponseObserver<HelloRequest, HelloReply> clientResponseObserver =
        new ClientResponseObserver<>() {
          ClientCallStreamObserver<HelloRequest> requestStream;
          @Override
          public void beforeStart(final ClientCallStreamObserver<HelloRequest> requestStream) {
            this.requestStream = requestStream;
            requestStream.disableAutoRequestWithInitial(1);
            requestStream.setOnReadyHandler(new Runnable() {

              final Iterator<String> iterator = names().iterator();

              @Override
              public void run() {

                while (requestStream.isReady()) {
                  if (iterator.hasNext()) {
                      String name = iterator.next();
                      logger.info("--> " + name);
                      HelloRequest request = HelloRequest.newBuilder().setName(name).build();
                      requestStream.onNext(request);
                  } else {
                      requestStream.onCompleted();
                  }
                }
              }
            });
          }

          @Override
          public void onNext(HelloReply value) {
            logger.info("<-- " + value.getMessage());
            // Signal the sender to send one message.
            requestStream.request(1);
          }

          @Override
          public void onError(Throwable t) {
            t.printStackTrace();
            done.countDown();
          }

          @Override
          public void onCompleted() {
            logger.info("All Done");
            done.countDown();
          }
        };


    stub.sayHelloStreaming(clientResponseObserver);
    done.await();
    channel.shutdown();
    channel.awaitTermination(1, TimeUnit.SECONDS);
  }

  private static List<String> names() {
    return Arrays.asList(
        "Sophia",
        "Jackson",
        "Emma",
        "Aiden",
        "Olivia",
        "Lucas",
        "Ava",
        "Liam",
        "Mia",
        "Noah",
        "Isabella",
        "Ethan",
        "Riley",
        "Mason",
        "Aria",
        "Caden",
        "Zoe",
        "Oliver",
        "Charlotte",
        "Elijah",
        "Lily",
        "Grayson",
        "Layla",
        "Jacob",
        "Amelia",
        "Michael",
        "Emily",
        "Benjamin",
        "Madelyn",
        "Carter",
        "Aubrey",
        "James",
        "Adalyn",
        "Jayden",
        "Madison",
        "Logan",
        "Chloe",
        "Alexander",
        "Harper",
        "Caleb",
        "Abigail",
        "Ryan",
        "Aaliyah",
        "Luke",
        "Avery",
        "Daniel",
        "Evelyn",
        "Jack",
        "Kaylee",
        "William",
        "Ella",
        "Owen",
        "Ellie",
        "Gabriel",
        "Scarlett",
        "Matthew",
        "Arianna",
        "Connor",
        "Hailey",
        "Jayce",
        "Nora",
        "Isaac",
        "Addison",
        "Sebastian",
        "Brooklyn",
        "Henry",
        "Hannah",
        "Muhammad",
        "Mila",
        "Cameron",
        "Leah",
        "Wyatt",
        "Elizabeth",
        "Dylan",
        "Sarah",
        "Nathan",
        "Eliana",
        "Nicholas",
        "Mackenzie",
        "Julian",
        "Peyton",
        "Eli",
        "Maria",
        "Levi",
        "Grace",
        "Isaiah",
        "Adeline",
        "Landon",
        "Elena",
        "David",
        "Anna",
        "Christian",
        "Victoria",
        "Andrew",
        "Camilla",
        "Brayden",
        "Lillian",
        "John",
        "Natalie",
        "Lincoln"
    );
  }
}
