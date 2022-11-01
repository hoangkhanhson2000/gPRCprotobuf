package com.example.grpcprotobuf.stock;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.example.grpcprotobuf.streaming.Stock;
import com.example.grpcprotobuf.streaming.StockQuote;
import com.example.grpcprotobuf.streaming.StockQuoteProviderGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class StockClient {
    private static final Logger logger = LoggerFactory.getLogger(StockClient.class.getName());

    private final StockQuoteProviderGrpc.StockQuoteProviderBlockingStub blockingStub;
    private final StockQuoteProviderGrpc.StockQuoteProviderStub nonBlockingStub;
    private List<Stock> stocks;

    public StockClient(Channel channel) {

        blockingStub = StockQuoteProviderGrpc.newBlockingStub(channel);
        nonBlockingStub = StockQuoteProviderGrpc.newStub(channel);
        initializeStocks();
    }

    public void serverSideStreamingListOfStockPrices() {

        logger.info("1.ServerSideStreaming - list of Stock prices from a given stock");
        Stock request = Stock.newBuilder()
                .setTickerSymbol("UNI")
                .setCompanyName("UniCloud Corp")
                .setDescription("server streaming ")
                .build();
        Iterator<StockQuote> stockQuotes;
        try {
            logger.info("REQUEST - ticker symbol {}", request.getTickerSymbol());
            stockQuotes = blockingStub.serverSideStreamingGetListStockQuotes(request);
            for (int i = 1; stockQuotes.hasNext(); i++) {
                StockQuote stockQuote = stockQuotes.next();
                logger.info("RESPONSE - Price #" + i + ": {}", stockQuote.getPrice());
            }
        } catch (StatusRuntimeException e) {
            logger.info("RPC failed: {}", e.getStatus());
        }
    }

    public void clientSideStreamingGetStatisticsOfStocks() throws InterruptedException {

        logger.info("2.ClientSideStreaming - getStatisticsOfStocks from a list of stocks");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<StockQuote> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(StockQuote summary) {
                logger.info("RESPONSE, got stock statistics - Average Price: {}, description: {}", summary.getPrice(), summary.getDescription());
            }

            @Override
            public void onCompleted() {
                logger.info("Finished clientSideStreamingGetStatisticsOfStocks");
                finishLatch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Stock Statistics Failed: {}", Status.fromThrowable(t));
                finishLatch.countDown();
            }
        };

        StreamObserver<Stock> requestObserver = nonBlockingStub.clientSideStreamingGetStatisticsOfStocks(responseObserver);
        try {

            for (Stock stock : stocks) {
                logger.info("REQUEST: {}, {}", stock.getTickerSymbol(), stock.getCompanyName());
                requestObserver.onNext(stock);
                if (finishLatch.getCount() == 0) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            logger.warn("clientSideStreamingGetStatisticsOfStocks can not finish within 1 minutes");
        }
    }

    public void bidirectionalStreamingGetListsStockQuotes() throws InterruptedException {

        logger.info("3.BidirectionalStreaming - getListsStockQuotes from list of stocks");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<StockQuote> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(StockQuote stockQuote) {
                logger.info("RESPONSE price#{} : {}, description:{}", stockQuote.getOfferNumber(), stockQuote.getPrice(), stockQuote.getDescription());
            }

            @Override
            public void onCompleted() {
                logger.info("Finished bidirectionalStreamingGetListsStockQuotes");
                finishLatch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("bidirectionalStreamingGetListsStockQuotes Failed: {}", Status.fromThrowable(t));
                finishLatch.countDown();
            }
        };
        StreamObserver<Stock> requestObserver = nonBlockingStub.bidirectionalStreamingGetListsStockQuotes(responseObserver);
        try {
            for (Stock stock : stocks) {
                logger.info("REQUEST: {}, {}", stock.getTickerSymbol(), stock.getCompanyName());
                requestObserver.onNext(stock);
                Thread.sleep(200);
                if (finishLatch.getCount() == 0) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();

        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            logger.warn("bidirectionalStreamingGetListsStockQuotes can not finish within 1 minute");
        }

    }

    public static void main(String[] args) throws InterruptedException {
        String target = "localhost:8980";
        if (args.length > 0) {
            target = args[0];
        }

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        try {
            StockClient client = new StockClient(channel);

            client.serverSideStreamingListOfStockPrices();

            client.clientSideStreamingGetStatisticsOfStocks();

            client.bidirectionalStreamingGetListsStockQuotes();

        } finally {
            channel.shutdownNow()
                    .awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private void initializeStocks() {

        this.stocks = Arrays.asList(
                Stock.newBuilder().setTickerSymbol("UNI").setCompanyName("UniCloud Corp").setDescription("UniCloud Tech").build()
                , Stock.newBuilder().setTickerSymbol("OMG").setCompanyName("OMG Corp").setDescription("OMG Intel").build()
                , Stock.newBuilder().setTickerSymbol("VNG").setCompanyName("VNG Corp").setDescription("VNG Intel").build()
                , Stock.newBuilder().setTickerSymbol("VTC").setCompanyName("VTC Corp").setDescription("VTC Intel").build()
                , Stock.newBuilder().setTickerSymbol("VTV").setCompanyName("VTV Corp").setDescription("VTV Intel").build());
    }
}