package com.example.grpcprotobuf.stock;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.example.grpcprotobuf.streaming.Stock;
import com.example.grpcprotobuf.streaming.StockQuote;
import com.example.grpcprotobuf.streaming.StockQuoteProviderGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class StockServer {

    private static final Logger logger = LoggerFactory.getLogger(StockClient.class.getName());
    private final int port;
    private final Server server;

    public StockServer(int port) {
        this.port = port;
        server = ServerBuilder.forPort(port)
                .addService(new StockService())
                .build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime()
                .addShutdownHook(new Thread(()->{}) {
                    @Override
                    public void run() {
                        System.err.println("shutting down server");
                        try {
                            StockServer.this.stop();
                        } catch (InterruptedException e) {
                            e.printStackTrace(System.err);
                        }
                        System.err.println("server shutted down");
                    }
                });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown()
                    .awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) throws Exception {
        StockServer stockServer = new StockServer(8980);
        stockServer.start();
        if (stockServer.server != null) {
            stockServer.server.awaitTermination();
        }
    }

    private static class StockService extends StockQuoteProviderGrpc.StockQuoteProviderImplBase {

        StockService() {
        }

        @Override
        public void serverSideStreamingGetListStockQuotes(Stock request, StreamObserver<StockQuote> responseObserver) {

            for (int i = 1; i <= 5; i++) {

                StockQuote stockQuote = StockQuote.newBuilder()
                        .setPrice(fetchStockPriceBid(request))
                        .setOfferNumber(i)
                        .setDescription("Price for stock:" + request.getTickerSymbol())
                        .build();
                responseObserver.onNext(stockQuote);
            }
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<Stock> clientSideStreamingGetStatisticsOfStocks(final StreamObserver<StockQuote> responseObserver) {
            return new StreamObserver<>() {
                int count;
                double price = 0.0;
                final StringBuffer sb = new StringBuffer();

                @Override
                public void onNext(Stock stock) {
                    count++;
                    price = fetchStockPriceBid(stock);
                    sb.append(":")
                            .append(stock.getTickerSymbol());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(StockQuote.newBuilder()
                            .setPrice(price / count)
                            .setDescription("Statistics-" + sb)
                            .build());
                    responseObserver.onCompleted();
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("error:{}", t.getMessage());
                }
            };
        }

        @Override
        public StreamObserver<Stock> bidirectionalStreamingGetListsStockQuotes(final StreamObserver<StockQuote> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(Stock request) {

                    for (int i = 1; i <= 6; i++) {

                        StockQuote stockQuote = StockQuote.newBuilder()
                                .setPrice(fetchStockPriceBid(request))
                                .setOfferNumber(i)
                                .setDescription("Price for stock:" + request.getTickerSymbol())
                                .build();
                        responseObserver.onNext(stockQuote);
                    }
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("error:{}", t.getMessage());
                }
            };
        }
    }

    private static double fetchStockPriceBid(Stock stock) {

        return stock.getTickerSymbol()
                .length()
                + ThreadLocalRandom.current()
                .nextDouble(-0.5d, 0.5d);
    }
}