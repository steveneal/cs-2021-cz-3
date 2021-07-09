package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        // use the TradeDataLoader to load the trade data archives
        TradeDataLoader tradeDataLoader = new TradeDataLoader();
        //trades = tradeDataLoader.loadTrades(session, "C:\\cs-case-study\\src\\test\\resources\\trades\\trades.json");
        trades = tradeDataLoader.loadTrades(session, new File("src/test/resources/trades/trades.json").getAbsolutePath());

        // take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new VolumeTradedByInstrumentOfEntityExtractor());
        extractors.add(new AverageTradedPriceExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        // Stream data from the input socket on localhost:9000
        JavaDStream<String> jsonStrings = streamingContext.socketTextStream("localhost", 9000);

        // convert each incoming string to a Rfq object
        JavaDStream<Rfq> rfqObjs = jsonStrings.map(x -> Rfq.fromJson(x));

        // process each Rfq object
        rfqObjs.foreachRDD(rdd -> {
            rdd.collect().forEach(rfq -> processRfq(rfq));
        });

        // start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        // get metadata from each of the extractors
        for (RfqMetadataExtractor extractor : extractors)
            for (Map.Entry<RfqMetadataFieldNames, Object> entry : extractor.extractMetaData(rfq, session, trades).entrySet())
                metadata.put(entry.getKey(), entry.getValue());

        //TODO: publish the metadata
        for (Map.Entry<RfqMetadataFieldNames, Object> entry : metadata.entrySet())
            Log.info(entry.getKey() + " - " + entry.getValue().toString());
    }
}
