package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.cs.rfq.decorator.kafka.IKafkaConstants.*;
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
        extractors.add(new AverageTradedPriceExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new TotalVolTradedWithEntityExtractor());
        extractors.add(new TradeSideBiasWithEntityExtractor());
        extractors.add(new VolumeTradedByInstrumentOfEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());

    }

    public void startSocketListener() throws InterruptedException {
        /*Dataset<String> df = session.readStream()
                                    .format("kafka")
                                    .option("kafka.bootstrap.servers", "localhost:9092")
                                    .option("subscribe", "bloomberg,euronext")
                                    .load()
                                    .selectExpr("CAST(value AS STRING)")
                                    .flatMap((FlatMapFunction<Row, String>) x -> Arrays.asList(x.getString(0)).iterator(), Encoders.STRING());

        try {
            df.writeStream()
                    .format("console")
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }*/


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", KAFKA_BROKERS);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", GROUP_ID_CONFIG);
        kafkaParams.put("auto.offset.reset", OFFSET_RESET_LATEST);
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("bloomberg", "euronext");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> data = stream.map(record -> record.value());
        //extStream.print();

        // Stream data from the input socket on localhost:9000
        //JavaDStream<String> jsonStrings = streamingContext.socketTextStream("localhost", 9000);

        // convert each incoming string to a Rfq object
        JavaDStream<Rfq> rfqObjs = data.map(x -> Rfq.fromJson(x));

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

        // publish the metadata
        MetadataJsonLogPublisher mjlp = new MetadataJsonLogPublisher();
        try {
            mjlp.publishMetadata(rfq, metadata);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
